#[cfg(feature = "extension")]
use ext_php_rs::prelude::*;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[cfg_attr(feature = "extension", php_class)]
pub struct FileProcessor;

#[derive(Clone)]
pub struct ColumnConfig {
    pub input_index: usize,
    pub output_index: usize,
    pub ops: Vec<Operation>,
    pub validate: Option<Validator>,
}

#[derive(Clone)]
pub enum Operation {
    Trim,
    DigitsOnly,
    Uppercase,
    Lowercase,
    PadLeft(usize, char),
    StripDdi(String),
}

impl Operation {
    pub fn apply(&self, s: &str) -> String {
        match self {
            Operation::Trim => s.trim().to_string(),
            Operation::DigitsOnly => s.chars().filter(|c| c.is_ascii_digit()).collect(),
            Operation::Uppercase => s.to_uppercase(),
            Operation::Lowercase => s.to_lowercase(),
            Operation::PadLeft(len, ch) => {
                let char_count = s.chars().count();
                if char_count >= *len {
                    s.to_string()
                } else {
                    let pad = ch.to_string().repeat(len - char_count);
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

pub fn parse_op(spec: &str) -> Option<Operation> {
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

#[derive(Clone)]
pub enum Validator {
    Cpf,
    PhoneBr,
}

impl Validator {
    pub fn check(&self, s: &str) -> bool {
        match self {
            Validator::Cpf => validate_cpf(s),
            Validator::PhoneBr => validate_phone_br(s),
        }
    }
}

pub fn validate_cpf(s: &str) -> bool {
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

pub fn validate_phone_br(s: &str) -> bool {
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

pub fn parse_columns(json: &str) -> Result<Vec<ColumnConfig>, String> {
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

// ==========================================================
// Lógica pura (sempre disponível, testável sem PHP runtime)
// ==========================================================

impl FileProcessor {
    /// Divide arquivo em N chunks em disco. Retorna contagem de linhas por chunk.
    pub fn split_file_impl(
        &self,
        input_path: &str,
        output_dir: &str,
        chunks: i64,
    ) -> Result<Vec<i64>, String> {
        let file = File::open(input_path).map_err(|e| e.to_string())?;
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

        std::fs::create_dir_all(output_dir).map_err(|e| e.to_string())?;

        let counts: Result<Vec<i64>, String> = (0..n)
            .into_par_iter()
            .map(|i| -> Result<i64, String> {
                let start = boundaries[i];
                let end = boundaries[i + 1];
                let slice = &data[start..end];
                let path = format!("{}/input_{}.csv", output_dir, i);
                let mut out = File::create(&path).map_err(|e| e.to_string())?;
                out.write_all(slice).map_err(|e| e.to_string())?;
                let mut count = slice.iter().filter(|&&b| b == b'\n').count() as i64;
                if !slice.is_empty() && *slice.last().unwrap() != b'\n' {
                    count += 1;
                }
                Ok(count)
            })
            .collect();

        counts
    }

    /// Processa arquivo único aplicando layout. Retorna [input, output, invalid].
    pub fn process_file_impl(
        &self,
        input_path: &str,
        output_path: &str,
        input_delimiter: &str,
        output_delimiter: &str,
        skip_header: bool,
        columns_json: &str,
    ) -> Result<Vec<i64>, String> {
        let columns = parse_columns(columns_json)?;
        let in_delim = input_delimiter.bytes().next().unwrap_or(b';');
        let out_delim = output_delimiter.chars().next().unwrap_or(';').to_string();

        let file = File::open(input_path).map_err(|e| e.to_string())?;
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
                    if let Some(v) = &col.validate {
                        if !v.check(&transformed) {
                            return None;
                        }
                    }
                    out_row[col.output_index] = transformed;
                }
                Some(out_row.join(&out_delim))
            })
            .collect();

        let valid: Vec<&String> = results.iter().filter_map(|r| r.as_ref()).collect();
        let output_count = valid.len() as i64;
        let invalid_count = input_count - output_count;

        let mut out = File::create(output_path).map_err(|e| e.to_string())?;
        for line in valid {
            writeln!(out, "{}", line).map_err(|e| e.to_string())?;
        }

        Ok(vec![input_count, output_count, invalid_count])
    }

    /// Processa todos os chunks em paralelo. Retorna [input, output, invalid] totais.
    pub fn process_chunks_impl(
        &self,
        dir: &str,
        chunks: i64,
        input_delimiter: &str,
        output_delimiter: &str,
        skip_header: bool,
        columns_json: &str,
    ) -> Result<Vec<i64>, String> {
        let columns = parse_columns(columns_json)?;
        let in_delim = input_delimiter.bytes().next().unwrap_or(b';') as char;
        let out_delim = output_delimiter.chars().next().unwrap_or(';').to_string();
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
                if skip_header && i == 0 && !lines.is_empty() {
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

    /// Concatena arquivos output_*.csv em um único arquivo final.
    pub fn merge_files_impl(
        &self,
        input_dir: &str,
        output_path: &str,
        chunks: i64,
    ) -> Result<i64, String> {
        let mut out = File::create(output_path).map_err(|e| e.to_string())?;
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

// ==========================================================
// Camada PHP (somente compilada com feature "extension")
// ==========================================================

#[cfg(feature = "extension")]
#[php_impl]
impl FileProcessor {
    pub fn __construct() -> Self {
        Self
    }

    pub fn split_file(
        &self,
        input_path: String,
        output_dir: String,
        chunks: i64,
    ) -> PhpResult<Vec<i64>> {
        self.split_file_impl(&input_path, &output_dir, chunks)
            .map_err(Into::into)
    }

    pub fn process_file(
        &self,
        input_path: String,
        output_path: String,
        input_delimiter: String,
        output_delimiter: String,
        skip_header: bool,
        columns_json: String,
    ) -> PhpResult<Vec<i64>> {
        self.process_file_impl(
            &input_path,
            &output_path,
            &input_delimiter,
            &output_delimiter,
            skip_header,
            &columns_json,
        )
        .map_err(Into::into)
    }

    pub fn process_chunks(
        &self,
        dir: String,
        chunks: i64,
        input_delimiter: String,
        output_delimiter: String,
        skip_header: bool,
        columns_json: String,
    ) -> PhpResult<Vec<i64>> {
        self.process_chunks_impl(
            &dir,
            chunks,
            &input_delimiter,
            &output_delimiter,
            skip_header,
            &columns_json,
        )
        .map_err(Into::into)
    }

    pub fn merge_files(
        &self,
        input_dir: String,
        output_path: String,
        chunks: i64,
    ) -> PhpResult<i64> {
        self.merge_files_impl(&input_dir, &output_path, chunks)
            .map_err(Into::into)
    }
}

#[cfg(feature = "extension")]
#[php_module]
pub fn module(module: ModuleBuilder) -> ModuleBuilder {
    module.class::<FileProcessor>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    // ===========================================================
    // Helpers
    // ===========================================================

    static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_temp_dir(name: &str) -> PathBuf {
        let id = DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "parallel_test_{}_{}_{}",
            name,
            std::process::id(),
            id
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_file(path: &PathBuf, content: &str) {
        fs::write(path, content).unwrap();
    }

    fn read_file(path: &PathBuf) -> String {
        fs::read_to_string(path).unwrap()
    }

    fn path_str(p: &PathBuf) -> &str {
        p.to_str().unwrap()
    }

    // ===========================================================
    // validate_cpf
    // ===========================================================

    #[test]
    fn cpf_accepts_valid_cpf_plain_digits() {
        // 12345678909 is a mathematically valid CPF (check digits 09)
        assert!(validate_cpf("12345678909"));
    }

    #[test]
    fn cpf_accepts_valid_cpf_with_formatting() {
        // filter(is_ascii_digit) strips non-digits
        assert!(validate_cpf("123.456.789-09"));
    }

    #[test]
    fn cpf_rejects_wrong_length() {
        assert!(!validate_cpf(""));
        assert!(!validate_cpf("123"));
        assert!(!validate_cpf("1234567890"));
        assert!(!validate_cpf("123456789012"));
    }

    #[test]
    fn cpf_rejects_all_same_digits() {
        assert!(!validate_cpf("00000000000"));
        assert!(!validate_cpf("11111111111"));
        assert!(!validate_cpf("99999999999"));
    }

    #[test]
    fn cpf_rejects_wrong_first_check_digit() {
        assert!(!validate_cpf("12345678919"));
    }

    #[test]
    fn cpf_rejects_wrong_second_check_digit() {
        assert!(!validate_cpf("12345678900"));
    }

    // ===========================================================
    // validate_phone_br
    // ===========================================================

    #[test]
    fn phone_accepts_landline_10_digits() {
        assert!(validate_phone_br("1122334455"));
    }

    #[test]
    fn phone_accepts_cellphone_11_digits_with_9() {
        assert!(validate_phone_br("11987654321"));
    }

    #[test]
    fn phone_rejects_cellphone_without_leading_9() {
        assert!(!validate_phone_br("11887654321"));
    }

    #[test]
    fn phone_rejects_ddd_starting_with_zero() {
        assert!(!validate_phone_br("0122334455"));
        assert!(!validate_phone_br("01987654321"));
    }

    #[test]
    fn phone_rejects_wrong_length() {
        assert!(!validate_phone_br(""));
        assert!(!validate_phone_br("123"));
        assert!(!validate_phone_br("123456789"));
        assert!(!validate_phone_br("123456789012"));
    }

    #[test]
    fn phone_strips_non_digits_before_validating() {
        assert!(validate_phone_br("(11) 98765-4321"));
    }

    // ===========================================================
    // Operation::apply
    // ===========================================================

    #[test]
    fn op_trim_removes_surrounding_whitespace() {
        assert_eq!(Operation::Trim.apply("  hello  "), "hello");
        assert_eq!(Operation::Trim.apply("\t\nfoo\n "), "foo");
    }

    #[test]
    fn op_digits_only_keeps_ascii_digits() {
        assert_eq!(Operation::DigitsOnly.apply("(11) 98765-4321"), "11987654321");
        assert_eq!(Operation::DigitsOnly.apply("abc123def"), "123");
        assert_eq!(Operation::DigitsOnly.apply(""), "");
    }

    #[test]
    fn op_uppercase_converts_to_upper() {
        assert_eq!(Operation::Uppercase.apply("hello"), "HELLO");
    }

    #[test]
    fn op_lowercase_converts_to_lower() {
        assert_eq!(Operation::Lowercase.apply("HELLO"), "hello");
    }

    #[test]
    fn op_pad_left_pads_when_shorter() {
        assert_eq!(Operation::PadLeft(5, '0').apply("42"), "00042");
    }

    #[test]
    fn op_pad_left_noop_when_equal_or_longer() {
        assert_eq!(Operation::PadLeft(3, '0').apply("abc"), "abc");
        assert_eq!(Operation::PadLeft(2, '0').apply("abcd"), "abcd");
    }

    #[test]
    fn op_pad_left_counts_characters_not_bytes() {
        // "á" são 2 bytes em UTF-8 mas 1 char. Pad para len=4 deve gerar 3 chars de pad.
        let padded = Operation::PadLeft(4, '0').apply("á");
        assert_eq!(padded, "000á");
        assert_eq!(padded.chars().count(), 4);
    }

    #[test]
    fn op_strip_ddi_removes_matching_prefix() {
        assert_eq!(
            Operation::StripDdi("55".into()).apply("5511987654321"),
            "11987654321"
        );
    }

    #[test]
    fn op_strip_ddi_noop_when_prefix_missing() {
        assert_eq!(
            Operation::StripDdi("55".into()).apply("11987654321"),
            "11987654321"
        );
    }

    #[test]
    fn op_strip_ddi_noop_when_string_equals_prefix() {
        // Comportamento atual: só remove se s.len() > ddi.len()
        assert_eq!(Operation::StripDdi("55".into()).apply("55"), "55");
    }

    // ===========================================================
    // parse_op
    // ===========================================================

    #[test]
    fn parse_op_trim() {
        assert!(matches!(parse_op("trim"), Some(Operation::Trim)));
    }

    #[test]
    fn parse_op_digits_only() {
        assert!(matches!(parse_op("digits_only"), Some(Operation::DigitsOnly)));
    }

    #[test]
    fn parse_op_uppercase_lowercase() {
        assert!(matches!(parse_op("uppercase"), Some(Operation::Uppercase)));
        assert!(matches!(parse_op("lowercase"), Some(Operation::Lowercase)));
    }

    #[test]
    fn parse_op_pad_left_with_args() {
        match parse_op("pad_left:11:0") {
            Some(Operation::PadLeft(len, ch)) => {
                assert_eq!(len, 11);
                assert_eq!(ch, '0');
            }
            _ => panic!("expected PadLeft"),
        }
    }

    #[test]
    fn parse_op_pad_left_missing_args_returns_none() {
        assert!(parse_op("pad_left").is_none());
        assert!(parse_op("pad_left:notanumber:0").is_none());
    }

    #[test]
    fn parse_op_strip_ddi_with_value() {
        match parse_op("strip_ddi:55") {
            Some(Operation::StripDdi(ddi)) => assert_eq!(ddi, "55"),
            _ => panic!("expected StripDdi"),
        }
    }

    #[test]
    fn parse_op_unknown_returns_none() {
        assert!(parse_op("trimm").is_none());
        assert!(parse_op("").is_none());
    }

    // ===========================================================
    // parse_columns
    // ===========================================================

    #[test]
    fn parse_columns_basic() {
        let json = r#"[{"in":0,"out":1,"ops":["trim","uppercase"]}]"#;
        let cols = parse_columns(json).unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].input_index, 0);
        assert_eq!(cols[0].output_index, 1);
        assert_eq!(cols[0].ops.len(), 2);
        assert!(cols[0].validate.is_none());
    }

    #[test]
    fn parse_columns_with_cpf_validator() {
        let json = r#"[{"in":0,"out":0,"ops":[],"validate":"cpf"}]"#;
        let cols = parse_columns(json).unwrap();
        assert!(matches!(cols[0].validate, Some(Validator::Cpf)));
    }

    #[test]
    fn parse_columns_with_phone_validator() {
        let json = r#"[{"in":0,"out":0,"ops":[],"validate":"phone_br"}]"#;
        let cols = parse_columns(json).unwrap();
        assert!(matches!(cols[0].validate, Some(Validator::PhoneBr)));
    }

    #[test]
    fn parse_columns_unknown_validator_becomes_none() {
        // Comportamento atual: validator desconhecido é silenciosamente descartado
        let json = r#"[{"in":0,"out":0,"ops":[],"validate":"cnpj"}]"#;
        let cols = parse_columns(json).unwrap();
        assert!(cols[0].validate.is_none());
    }

    #[test]
    fn parse_columns_silently_ignores_unknown_ops() {
        // Comportamento atual: ops desconhecidas são descartadas via filter_map
        let json = r#"[{"in":0,"out":0,"ops":["trim","not_a_real_op","uppercase"]}]"#;
        let cols = parse_columns(json).unwrap();
        assert_eq!(cols[0].ops.len(), 2);
    }

    #[test]
    fn parse_columns_rejects_invalid_json() {
        assert!(parse_columns("not json").is_err());
        assert!(parse_columns("{}").is_err()); // não é array
    }

    #[test]
    fn parse_columns_rejects_missing_fields() {
        assert!(parse_columns(r#"[{"out":0,"ops":[]}]"#).is_err());
        assert!(parse_columns(r#"[{"in":0,"ops":[]}]"#).is_err());
        assert!(parse_columns(r#"[{"in":0,"out":0}]"#).is_err());
    }

    // ===========================================================
    // Validator::check
    // ===========================================================

    #[test]
    fn validator_cpf_delegates_to_validate_cpf() {
        assert!(Validator::Cpf.check("12345678909"));
        assert!(!Validator::Cpf.check("11111111111"));
    }

    #[test]
    fn validator_phone_delegates_to_validate_phone_br() {
        assert!(Validator::PhoneBr.check("11987654321"));
        assert!(!Validator::PhoneBr.check("01987654321"));
    }

    // ===========================================================
    // FileProcessor::split_file_impl
    // ===========================================================

    #[test]
    fn split_file_creates_expected_number_of_chunks() {
        let dir = unique_temp_dir("split_basic");
        let input = dir.join("input.csv");
        write_file(&input, "a,1\nb,2\nc,3\nd,4\ne,5\nf,6\n");

        let fp = FileProcessor;
        let counts = fp
            .split_file_impl(path_str(&input), path_str(&dir), 3)
            .unwrap();

        assert_eq!(counts.len(), 3);
        assert_eq!(counts.iter().sum::<i64>(), 6);
        assert!(dir.join("input_0.csv").exists());
        assert!(dir.join("input_1.csv").exists());
        assert!(dir.join("input_2.csv").exists());
    }

    #[test]
    fn split_file_respects_line_boundaries() {
        let dir = unique_temp_dir("split_boundaries");
        let input = dir.join("input.csv");
        write_file(&input, "aaaa\nbbbb\ncccc\ndddd\neeee\nffff\n");

        let fp = FileProcessor;
        fp.split_file_impl(path_str(&input), path_str(&dir), 2)
            .unwrap();

        let chunk0 = read_file(&dir.join("input_0.csv"));
        let chunk1 = read_file(&dir.join("input_1.csv"));
        assert!(chunk0.ends_with('\n'));
        assert!(chunk1.ends_with('\n'));
        assert_eq!(
            format!("{}{}", chunk0, chunk1),
            "aaaa\nbbbb\ncccc\ndddd\neeee\nffff\n"
        );
    }

    #[test]
    fn split_file_with_single_chunk() {
        let dir = unique_temp_dir("split_single");
        let input = dir.join("input.csv");
        write_file(&input, "a\nb\nc\n");

        let fp = FileProcessor;
        let counts = fp
            .split_file_impl(path_str(&input), path_str(&dir), 1)
            .unwrap();
        assert_eq!(counts, vec![3]);
        assert_eq!(read_file(&dir.join("input_0.csv")), "a\nb\nc\n");
    }

    #[test]
    fn split_file_counts_last_line_without_trailing_newline() {
        let dir = unique_temp_dir("split_no_trailing");
        let input = dir.join("input.csv");
        write_file(&input, "a\nb\nc"); // sem \n final

        let fp = FileProcessor;
        let counts = fp
            .split_file_impl(path_str(&input), path_str(&dir), 1)
            .unwrap();
        assert_eq!(counts, vec![3]);
    }

    // ===========================================================
    // FileProcessor::process_file_impl
    // ===========================================================

    #[test]
    fn process_file_applies_operations() {
        let dir = unique_temp_dir("proc_file_ops");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "  hello ;world\n foo ;bar\n");

        let fp = FileProcessor;
        let res = fp
            .process_file_impl(
                path_str(&input),
                path_str(&output),
                ";",
                ";",
                false,
                r#"[
                    {"in":0,"out":0,"ops":["trim","uppercase"]},
                    {"in":1,"out":1,"ops":["trim"]}
                ]"#,
            )
            .unwrap();

        assert_eq!(res, vec![2, 2, 0]);
        let out = read_file(&output);
        assert!(out.contains("HELLO;world"));
        assert!(out.contains("FOO;bar"));
    }

    #[test]
    fn process_file_skips_header_when_flag_set() {
        let dir = unique_temp_dir("proc_file_header");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "name;age\nalice;30\nbob;25\n");

        let fp = FileProcessor;
        let res = fp
            .process_file_impl(
                path_str(&input),
                path_str(&output),
                ";",
                ";",
                true,
                r#"[{"in":0,"out":0,"ops":[]}]"#,
            )
            .unwrap();
        assert_eq!(res[0], 2); // input_count exclui header
    }

    #[test]
    fn process_file_filters_invalid_rows_via_validator() {
        // process_file deve rodar os validators e descartar linhas inválidas.
        let dir = unique_temp_dir("proc_file_validate");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "11111111111\n12345678909\n");

        let fp = FileProcessor;
        let res = fp
            .process_file_impl(
                path_str(&input),
                path_str(&output),
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":[],"validate":"cpf"}]"#,
            )
            .unwrap();
        assert_eq!(res, vec![2, 1, 1]);
        let out = read_file(&output);
        assert!(out.contains("12345678909"));
        assert!(!out.contains("11111111111"));
    }

    #[test]
    fn process_file_drops_empty_lines_current_behavior() {
        let dir = unique_temp_dir("proc_file_blank");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "a\n\nb\n");

        let fp = FileProcessor;
        let res = fp
            .process_file_impl(
                path_str(&input),
                path_str(&output),
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":[]}]"#,
            )
            .unwrap();
        assert_eq!(res[0], 2); // linha em branco fora do input_count
    }

    // ===========================================================
    // FileProcessor::process_chunks_impl
    // ===========================================================

    #[test]
    fn process_chunks_applies_ops_and_validates() {
        let dir = unique_temp_dir("proc_chunks_validate");
        write_file(&dir.join("input_0.csv"), "12345678909\n11111111111\n");
        write_file(&dir.join("input_1.csv"), "12345678909\n00000000000\n");

        let fp = FileProcessor;
        let res = fp
            .process_chunks_impl(
                path_str(&dir),
                2,
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":[],"validate":"cpf"}]"#,
            )
            .unwrap();

        assert_eq!(res, vec![4, 2, 2]);
    }

    #[test]
    fn process_chunks_writes_output_per_chunk() {
        let dir = unique_temp_dir("proc_chunks_out");
        write_file(&dir.join("input_0.csv"), "alice;30\n");
        write_file(&dir.join("input_1.csv"), "bob;25\n");

        let fp = FileProcessor;
        fp.process_chunks_impl(
            path_str(&dir),
            2,
            ";",
            ",",
            false,
            r#"[{"in":0,"out":0,"ops":["uppercase"]},{"in":1,"out":1,"ops":[]}]"#,
        )
        .unwrap();

        assert_eq!(read_file(&dir.join("output_0.csv")), "ALICE,30\n");
        assert_eq!(read_file(&dir.join("output_1.csv")), "BOB,25\n");
    }

    #[test]
    fn process_chunks_handles_missing_chunk_gracefully() {
        let dir = unique_temp_dir("proc_chunks_missing");
        write_file(&dir.join("input_0.csv"), "a\n");
        // input_1.csv ausente

        let fp = FileProcessor;
        let res = fp
            .process_chunks_impl(
                path_str(&dir),
                2,
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":[]}]"#,
            )
            .unwrap();
        assert_eq!(res, vec![1, 1, 0]);
    }

    // ===========================================================
    // FileProcessor::merge_files_impl
    // ===========================================================

    #[test]
    fn merge_files_concatenates_chunks_in_order() {
        let dir = unique_temp_dir("merge_ordered");
        write_file(&dir.join("output_0.csv"), "line1\nline2\n");
        write_file(&dir.join("output_1.csv"), "line3\n");
        write_file(&dir.join("output_2.csv"), "line4\nline5\n");

        let final_path = dir.join("final.csv");
        let fp = FileProcessor;
        let total = fp
            .merge_files_impl(path_str(&dir), path_str(&final_path), 3)
            .unwrap();

        assert_eq!(total, 5);
        assert_eq!(
            read_file(&final_path),
            "line1\nline2\nline3\nline4\nline5\n"
        );
    }

    #[test]
    fn merge_files_skips_missing_chunks() {
        let dir = unique_temp_dir("merge_missing");
        write_file(&dir.join("output_0.csv"), "a\n");
        // output_1.csv ausente
        write_file(&dir.join("output_2.csv"), "c\n");

        let final_path = dir.join("final.csv");
        let fp = FileProcessor;
        let total = fp
            .merge_files_impl(path_str(&dir), path_str(&final_path), 3)
            .unwrap();
        assert_eq!(total, 2);
        assert_eq!(read_file(&final_path), "a\nc\n");
    }

    // ===========================================================
    // Pipeline completo
    // ===========================================================

    #[test]
    fn full_pipeline_split_process_merge_without_header() {
        // Pipeline correto quando o input já não tem header: skip_header=false.
        // Alice e Carol têm CPF válido; Bob tem CPF inválido (11111111111).
        let dir = unique_temp_dir("pipeline_noheader");
        let input = dir.join("input.csv");
        let final_out = dir.join("final.csv");
        write_file(
            &input,
            "12345678909;Alice\n11111111111;Bob\n12345678909;Carol\n",
        );

        let fp = FileProcessor;
        fp.split_file_impl(path_str(&input), path_str(&dir), 2)
            .unwrap();

        let res = fp
            .process_chunks_impl(
                path_str(&dir),
                2,
                ";",
                ";",
                false,
                r#"[
                    {"in":0,"out":0,"ops":[],"validate":"cpf"},
                    {"in":1,"out":1,"ops":["uppercase"]}
                ]"#,
            )
            .unwrap();

        assert_eq!(res, vec![3, 2, 1]);

        let total = fp
            .merge_files_impl(path_str(&dir), path_str(&final_out), 2)
            .unwrap();
        assert_eq!(total, 2);

        let final_content = read_file(&final_out);
        assert!(final_content.contains("ALICE"));
        assert!(final_content.contains("CAROL"));
        assert!(!final_content.contains("Bob"));
    }

    #[test]
    fn full_pipeline_skip_header_only_drops_from_first_chunk() {
        // skip_header=true deve remover o header apenas do chunk 0,
        // preservando linhas úteis no início dos chunks seguintes.
        let dir = unique_temp_dir("pipeline_skip_header");
        let input = dir.join("input.csv");
        write_file(
            &input,
            "header1;header2\n12345678909;Alice\n11111111111;Bob\n12345678909;Carol\n",
        );

        let fp = FileProcessor;
        fp.split_file_impl(path_str(&input), path_str(&dir), 2)
            .unwrap();

        let res = fp
            .process_chunks_impl(
                path_str(&dir),
                2,
                ";",
                ";",
                true,
                r#"[{"in":0,"out":0,"ops":[],"validate":"cpf"}]"#,
            )
            .unwrap();

        // Boundary cai após "...Bob\n". Chunks:
        //   chunk 0: header + Alice + Bob  (skip_header descarta header)
        //     -> Alice válida, Bob inválido -> output 1
        //   chunk 1: Carol  (skip_header NÃO se aplica a chunks > 0)
        //     -> Carol válida -> output 1
        // Total output: 2.
        assert_eq!(res, vec![3, 2, 1]);
    }
}
