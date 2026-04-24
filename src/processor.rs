#[cfg(feature = "extension")]
use ext_php_rs::prelude::*;
use memmap2::Mmap;
use rayon::prelude::*;
use std::borrow::Cow;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::layout::{parse_columns, ColumnConfig};
use crate::operations::apply_op;

#[cfg_attr(feature = "extension", php_class)]
pub struct FileProcessor;

// ==========================================================
// Helpers CSV (privados ao módulo)
// ==========================================================

const CSV_WRITE_BUFFER: usize = 1024 * 1024;

fn max_output_index(columns: &[ColumnConfig]) -> usize {
    columns.iter().map(|c| c.output_index).max().unwrap_or(0) + 1
}

fn parse_delimiter(delim: &str, default: u8) -> u8 {
    delim.bytes().next().unwrap_or(default)
}

fn csv_reader<R: std::io::Read>(delim: u8, has_headers: bool, reader: R) -> csv::Reader<R> {
    csv::ReaderBuilder::new()
        .delimiter(delim)
        .has_headers(has_headers)
        .flexible(true)
        .from_reader(reader)
}

fn csv_writer<W: std::io::Write>(delim: u8, writer: W) -> csv::Writer<W> {
    csv::WriterBuilder::new()
        .delimiter(delim)
        .terminator(csv::Terminator::Any(b'\n'))
        .buffer_capacity(CSV_WRITE_BUFFER)
        .from_writer(writer)
}

/// Aplica o layout numa record e escreve a saída se for válida.
/// Retorna `Ok(true)` se a linha foi escrita, `Ok(false)` se foi descartada
/// por um validator, e `Err` apenas se o writer CSV falhar.
fn transform_and_write<W: std::io::Write>(
    record: &csv::StringRecord,
    columns: &[ColumnConfig],
    out_row: &mut [String],
    wtr: &mut csv::Writer<W>,
) -> csv::Result<bool> {
    for slot in out_row.iter_mut() {
        slot.clear();
    }
    for col in columns {
        let val = record.get(col.input_index).unwrap_or("");
        let mut transformed: Cow<'_, str> = Cow::Borrowed(val);
        for op in &col.ops {
            transformed = apply_op(op, transformed);
        }
        if let Some(v) = &col.validate {
            if !v.check(&transformed) {
                return Ok(false);
            }
        }
        out_row[col.output_index].push_str(&transformed);
    }
    wtr.write_record(out_row.iter())?;
    Ok(true)
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
        let in_delim = parse_delimiter(input_delimiter, b';');
        let out_delim = parse_delimiter(output_delimiter, b';');
        let max_out = max_output_index(&columns);

        let file = File::open(input_path).map_err(|e| e.to_string())?;
        let mmap = unsafe { Mmap::map(&file).map_err(|e| e.to_string())? };
        let data: &[u8] = &mmap;

        let mut rdr = csv_reader(in_delim, skip_header, data);
        let out_file = File::create(output_path).map_err(|e| e.to_string())?;
        let mut wtr = csv_writer(out_delim, out_file);

        let mut input_count = 0i64;
        let mut output_count = 0i64;
        let mut out_row: Vec<String> = vec![String::new(); max_out];

        for result in rdr.records() {
            let record = result.map_err(|e| e.to_string())?;
            input_count += 1;
            let written = transform_and_write(&record, &columns, &mut out_row, &mut wtr)
                .map_err(|e| e.to_string())?;
            if written {
                output_count += 1;
            }
        }
        wtr.flush().map_err(|e| e.to_string())?;

        Ok(vec![input_count, output_count, input_count - output_count])
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
        let in_delim = parse_delimiter(input_delimiter, b';');
        let out_delim = parse_delimiter(output_delimiter, b';');
        let n = chunks as usize;
        let max_out = max_output_index(&columns);

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

                let has_headers = skip_header && i == 0;
                let mut rdr = csv_reader(in_delim, has_headers, data);

                let out_file = match File::create(&out_path) {
                    Ok(f) => f,
                    Err(_) => return (0, 0, 0),
                };
                let mut wtr = csv_writer(out_delim, out_file);

                let mut input_count = 0i64;
                let mut output_count = 0i64;
                let mut out_row: Vec<String> = vec![String::new(); max_out];

                for result in rdr.records() {
                    let record = match result {
                        Ok(r) => r,
                        Err(_) => continue,
                    };
                    input_count += 1;
                    match transform_and_write(&record, &columns, &mut out_row, &mut wtr) {
                        Ok(true) => output_count += 1,
                        Ok(false) => {} // descartada por validator
                        Err(_) => break, // falha ao escrever
                    }
                }
                let _ = wtr.flush();

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

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
    // split_file_impl
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
    // process_file_impl
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
    fn process_file_handles_quoted_fields_with_embedded_delimiter() {
        let dir = unique_temp_dir("proc_file_quoted");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "1;\"Silva; Júnior\";42\n2;Souza;30\n");

        let fp = FileProcessor;
        let res = fp
            .process_file_impl(
                path_str(&input),
                path_str(&output),
                ";",
                ";",
                false,
                r#"[
                    {"in":0,"out":0,"ops":[]},
                    {"in":1,"out":1,"ops":["uppercase"]},
                    {"in":2,"out":2,"ops":[]}
                ]"#,
            )
            .unwrap();

        assert_eq!(res, vec![2, 2, 0]);
        let out = read_file(&output);
        assert!(out.contains("\"SILVA; JÚNIOR\""));
        assert!(out.contains("SOUZA"));
    }

    #[test]
    fn process_file_handles_quoted_field_with_embedded_quote() {
        let dir = unique_temp_dir("proc_file_quote_in_quote");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "1;\"he said \"\"hi\"\"\"\n");

        let fp = FileProcessor;
        let res = fp
            .process_file_impl(
                path_str(&input),
                path_str(&output),
                ";",
                ";",
                false,
                r#"[{"in":1,"out":0,"ops":[]}]"#,
            )
            .unwrap();

        assert_eq!(res[0], 1);
        let out = read_file(&output);
        assert!(out.contains("he said \"\"hi\"\""));
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
    // process_chunks_impl
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
    // merge_files_impl
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

        // chunk 0: header + Alice + Bob (skip_header descarta header)
        //   -> Alice válida, Bob inválido -> output 1
        // chunk 1: Carol (skip_header NÃO se aplica a chunks > 0)
        //   -> Carol válida -> output 1
        assert_eq!(res, vec![3, 2, 1]);
    }
}
