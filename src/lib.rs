use ext_php_rs::prelude::*;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[php_class]
pub struct FileProcessor;

#[derive(Clone)]
struct ColumnConfig {
    input_index: usize,
    output_index: usize,
    ops: Vec<Operation>,
    validate: Option<Validator>,
}

#[derive(Clone)]
enum Operation {
    Trim,
    DigitsOnly,
    Uppercase,
    Lowercase,
    PadLeft(usize, char),
    StripDdi(String),
}

impl Operation {
    fn apply(&self, s: &str) -> String {
        match self {
            Operation::Trim => s.trim().to_string(),
            Operation::DigitsOnly => s.chars().filter(|c| c.is_ascii_digit()).collect(),
            Operation::Uppercase => s.to_uppercase(),
            Operation::Lowercase => s.to_lowercase(),
            Operation::PadLeft(len, ch) => {
                if s.len() >= *len {
                    s.to_string()
                } else {
                    let pad = ch.to_string().repeat(len - s.len());
                    format!("{}{}", pad, s)
                }
            }
            Operation::StripDdi(ddi) => {
                if s.starts_with(ddi) && s.len() > ddi.len() {
                    s[ddi.len()..].to_string()
                } else {
                    s.to_string()
                }
            }
        }
    }
}

fn parse_op(spec: &str) -> Option<Operation> {
    let parts: Vec<&str> = spec.splitn(3, ':').collect();
    match parts[0] {
        "trim" => Some(Operation::Trim),
        "digits_only" => Some(Operation::DigitsOnly),
        "uppercase" => Some(Operation::Uppercase),
        "lowercase" => Some(Operation::Lowercase),
        "pad_left" => {
            let len = parts.get(1)?.parse().ok()?;
            let ch = parts.get(2)?.chars().next().unwrap_or('0');
            Some(Operation::PadLeft(len, ch))
        }
        "strip_ddi" => Some(Operation::StripDdi(parts.get(1)?.to_string())),
        _ => None,
    }
}

#[php_impl]
impl FileProcessor {
    pub fn __construct() -> Self {
        Self
    }

    /// Divide arquivo em N chunks de entrada em disco (substituto do calculateChunks+processChunk)
    /// Retorna array com [total_lines, lines_per_chunk]
    pub fn split_file(
        &self,
        input_path: String,
        output_dir: String,
        chunks: i64,
    ) -> PhpResult<Vec<i64>> {
        let file = File::open(&input_path).map_err(|e| e.to_string())?;
        let mmap = unsafe { Mmap::map(&file).map_err(|e| e.to_string())? };
        let data: &[u8] = &mmap;
        let len = data.len();
        let n = chunks.max(1) as usize;

        let mut boundaries = Vec::with_capacity(n + 1);
        boundaries.push(0usize);
        for i in 1..n {
            let approx = len * i / n;
            let adjusted = match data[approx..].iter().position(|&b| b == b'\n') {
                Some(off) => approx + off + 1,
                None => len,
            };
            boundaries.push(adjusted);
        }
        boundaries.push(len);

        std::fs::create_dir_all(&output_dir).map_err(|e| e.to_string())?;

        let counts: Vec<i64> = (0..n)
            .into_par_iter()
            .map(|i| {
                let start = boundaries[i];
                let end = boundaries[i + 1];
                let slice = &data[start..end];
                let path = format!("{}/input_{}.csv", output_dir, i);
                let mut out = File::create(&path).expect("create chunk");
                out.write_all(slice).expect("write chunk");
                let mut count = slice.iter().filter(|&&b| b == b'\n').count() as i64;
                if !slice.is_empty() && *slice.last().unwrap() != b'\n' {
                    count += 1;
                }
                count
            })
            .collect();

        Ok(counts)
    }

    /// Processa arquivo de entrada aplicando layout e gera arquivo de saída
    /// Retorna [input_count, output_count, invalid_count]
    pub fn process_file(
        &self,
        input_path: String,
        output_path: String,
        input_delimiter: String,
        output_delimiter: String,
        skip_header: bool,
        columns_json: String,
    ) -> PhpResult<Vec<i64>> {
        let columns = parse_columns(&columns_json)?;
        let in_delim = input_delimiter.bytes().next().unwrap_or(b';');
        let out_delim = output_delimiter.chars().next().unwrap_or(';').to_string();

        let file = File::open(&input_path).map_err(|e| e.to_string())?;
        let mmap = unsafe { Mmap::map(&file).map_err(|e| e.to_string())? };
        let data: &[u8] = &mmap;

        let mut lines: Vec<&[u8]> = data
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();
        if skip_header && !lines.is_empty() {
            lines.remove(0);
        }
        let input_count = lines.len() as i64;

        let max_out = columns.iter().map(|c| c.output_index).max().unwrap_or(0) + 1;

        let results: Vec<Option<String>> = lines
            .par_iter()
            .map(|line| {
                let fields: Vec<&str> = std::str::from_utf8(line)
                    .ok()?
                    .split(in_delim as char)
                    .collect();
                let mut out_row: Vec<String> = vec![String::new(); max_out];
                for col in &columns {
                    let val = fields.get(col.input_index).copied().unwrap_or("");
                    let mut transformed = val.to_string();
                    for op in &col.ops {
                        transformed = op.apply(&transformed);
                    }
                    out_row[col.output_index] = transformed;
                }
                Some(out_row.join(&out_delim))
            })
            .collect();

        let valid: Vec<&String> = results.iter().filter_map(|r| r.as_ref()).collect();
        let output_count = valid.len() as i64;
        let invalid_count = input_count - output_count;

        let mut out = File::create(&output_path).map_err(|e| e.to_string())?;
        for line in valid {
            writeln!(out, "{}", line).map_err(|e| e.to_string())?;
        }

        Ok(vec![input_count, output_count, invalid_count])
    }

    /// Processa todos os chunks em paralelo numa chamada só
    /// Retorna [input_total, output_total, invalid_total]
    pub fn process_chunks(
        &self,
        dir: String,
        chunks: i64,
        input_delimiter: String,
        output_delimiter: String,
        skip_header: bool,
        columns_json: String,
    ) -> PhpResult<Vec<i64>> {
        let columns = parse_columns(&columns_json)?;
        let in_delim = input_delimiter.bytes().next().unwrap_or(b';') as char;
        let out_delim = output_delimiter.chars().next().unwrap_or(';').to_string();
        eprintln!("out_delim = {:?}", out_delim);
        let n = chunks as usize;
        let max_out = columns.iter().map(|c| c.output_index).max().unwrap_or(0) + 1;

        let results: Vec<(i64, i64, i64)> = (0..n)
            .into_par_iter()
            .map(|i| {
                let in_path = format!("{}/input_{}.csv", dir, i);
                let out_path = format!("{}/output_{}.csv", dir, i);

                let file = match File::open(&in_path) {
                    Ok(f) => f,
                    Err(_) => return (0, 0, 0),
                };
                let mmap = match unsafe { Mmap::map(&file) } {
                    Ok(m) => m,
                    Err(_) => return (0, 0, 0),
                };
                let data: &[u8] = &mmap;

                let mut lines: Vec<&[u8]> = data
                    .split(|&b| b == b'\n')
                    .filter(|l| !l.is_empty())
                    .collect();
                if skip_header && !lines.is_empty() {
                    lines.remove(0);
                }
                let input_count = lines.len() as i64;

                let mut out = match File::create(&out_path) {
                    Ok(f) => f,
                    Err(_) => return (input_count, 0, input_count),
                };
                let mut buf = String::with_capacity(1024 * 1024);
                let mut output_count = 0i64;

                for line in &lines {
                    let s = match std::str::from_utf8(line) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let fields: Vec<&str> = s.split(in_delim).collect();
                    let mut out_row: Vec<String> = vec![String::new(); max_out];
                    let mut valid = true;
                    for col in &columns {
                        let val = fields.get(col.input_index).copied().unwrap_or("");
                        let mut t = val.to_string();
                        for op in &col.ops {
                            t = op.apply(&t);
                        }
                        if let Some(v) = &col.validate {
                            if !v.check(&t) {
                                valid = false;
                                break;
                            }
                        }
                        out_row[col.output_index] = t;
                    }
                    if !valid {
                        continue;
                    }
                    buf.push_str(&out_row.join(&out_delim));
                    buf.push('\n');
                    output_count += 1;

                    if buf.len() > 1024 * 1024 {
                        let _ = out.write_all(buf.as_bytes());
                        buf.clear();
                    }
                }
                if !buf.is_empty() {
                    let _ = out.write_all(buf.as_bytes());
                }

                (input_count, output_count, input_count - output_count)
            })
            .collect();

        let mut totals = (0i64, 0i64, 0i64);
        for r in results {
            totals.0 += r.0;
            totals.1 += r.1;
            totals.2 += r.2;
        }
        Ok(vec![totals.0, totals.1, totals.2])
    }

    /// Concatena arquivos output_*.csv em um único arquivo final
    pub fn merge_files(
        &self,
        input_dir: String,
        output_path: String,
        chunks: i64,
    ) -> PhpResult<i64> {
        let mut out = File::create(&output_path).map_err(|e| e.to_string())?;
        let mut total: i64 = 0;
        for i in 0..chunks {
            let path = format!("{}/output_{}.csv", input_dir, i);
            if !Path::new(&path).exists() {
                continue;
            }
            let data = std::fs::read(&path).map_err(|e| e.to_string())?;
            total += data.iter().filter(|&&b| b == b'\n').count() as i64;
            out.write_all(&data).map_err(|e| e.to_string())?;
        }
        Ok(total)
    }
}

fn parse_columns(json: &str) -> PhpResult<Vec<ColumnConfig>> {
    let parsed: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| format!("invalid json: {}", e))?;
    let arr = parsed.as_array().ok_or("expected array")?;
    let mut cols = Vec::new();
    for item in arr {
        let in_idx = item["in"].as_u64().ok_or("missing 'in'")? as usize;
        let out_idx = item["out"].as_u64().ok_or("missing 'out'")? as usize;
        let ops_arr = item["ops"].as_array().ok_or("missing 'ops'")?;
        let ops: Vec<Operation> = ops_arr
            .iter()
            .filter_map(|v| v.as_str())
            .filter_map(parse_op)
            .collect();
        let validate = item["validate"].as_str().and_then(|s| match s {
            "cpf" => Some(Validator::Cpf),
            "phone_br" => Some(Validator::PhoneBr),
            _ => None,
        });
        cols.push(ColumnConfig {
            input_index: in_idx,
            output_index: out_idx,
            ops,
            validate,
        });
    }
    Ok(cols)
}

#[derive(Clone)]
enum Validator {
    Cpf,
    PhoneBr,
}

impl Validator {
    fn check(&self, s: &str) -> bool {
        match self {
            Validator::Cpf => validate_cpf(s),
            Validator::PhoneBr => validate_phone_br(s),
        }
    }
}

fn validate_cpf(s: &str) -> bool {
    let digits: Vec<u8> = s.bytes().filter(|b| b.is_ascii_digit()).map(|b| b - b'0').collect();
    if digits.len() != 11 {
        return false;
    }
    // rejeita todos iguais (00000000000, 11111111111, etc.)
    if digits.iter().all(|&d| d == digits[0]) {
        return false;
    }
    // primeiro dígito
    let sum: u32 = (0..9).map(|i| digits[i] as u32 * (10 - i as u32)).sum();
    let d1 = ((sum * 10) % 11) % 10;
    if d1 as u8 != digits[9] {
        return false;
    }
    // segundo dígito
    let sum: u32 = (0..10).map(|i| digits[i] as u32 * (11 - i as u32)).sum();
    let d2 = ((sum * 10) % 11) % 10;
    d2 as u8 == digits[10]
}

fn validate_phone_br(s: &str) -> bool {
    let digits: Vec<u8> = s.bytes().filter(|b| b.is_ascii_digit()).collect();
    let len = digits.len();
    // 10 (fixo) ou 11 (celular com 9)
    if len != 10 && len != 11 {
        return false;
    }
    // DDD válido (11-99, não começa com 0)
    if digits[0] == b'0' {
        return false;
    }
    // celular (11 dígitos) deve ter 9 na terceira posição
    if len == 11 && digits[2] != b'9' {
        return false;
    }
    true
}

#[php_module]
pub fn module(module: ModuleBuilder) -> ModuleBuilder {
    module.class::<FileProcessor>()
}