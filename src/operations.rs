use std::borrow::Cow;

use crate::validators::{validate_cnpj, validate_cpf};

#[derive(Clone, Debug)]
pub enum Operation {
    Trim,
    DigitsOnly,
    Uppercase,
    Lowercase,
    PadLeft(usize, char),
    StripDdi(String),
    /// Remove zeros à esquerda. `"0001234"` → `"1234"`, `"0000"` → `""`.
    RemoveLeadingZeroes,
    /// Canonicaliza CPF: tira não-dígitos, tira leading zeros, pega os
    /// últimos 11 dígitos (ou pad à esquerda com `0`), valida. Retorna o
    /// canônico (11 dígitos) ou `""` se inválido. Alinha com o pipe
    /// `digits|remove_leading_zeroes|right:11|pad_left:0,11|<validate_cpf>`
    /// do DataSanitizer PHP.
    CpfCanonical,
    /// Canonicaliza CNPJ: mesma lógica, 14 dígitos.
    CnpjCanonical,
    /// Detecta e canonicaliza documento brasileiro como CPF OU CNPJ.
    /// Se >11 dígitos após stripar zeros → tenta CNPJ (14 dig).
    /// Senão → tenta CPF (11 dig); se falhar, tenta CNPJ (14 dig).
    /// Retorna canônico válido ou `""`.
    DocumentCanonical,
    /// Ignora o valor de entrada e emite sempre o literal fornecido.
    /// Útil para injetar colunas constantes na saída (ex.: `tenant_id`,
    /// `job_id`, `created_at` fixo) sem que esses valores precisem existir
    /// no arquivo de entrada. Sintaxe no layout JSON: `"constant:<valor>"`.
    /// Tudo após o primeiro `:` vira parte do valor, então `"constant:a:b"`
    /// emite `"a:b"`.
    Constant(String),
}

/// Pega os últimos `n` chars de `s` (se len >= n) ou left-pad com `'0'`
/// até `n` chars. Usado pela normalização de documento.
fn to_n_digits(s: &str, n: usize) -> String {
    if s.len() >= n {
        s[s.len() - n..].to_string()
    } else {
        let pad = n - s.len();
        let mut out = String::with_capacity(n);
        for _ in 0..pad {
            out.push('0');
        }
        out.push_str(s);
        out
    }
}

impl Operation {
    /// Aplica a operação em `s`. Retorna `Cow::Borrowed` quando nada muda,
    /// evitando alocação no hot path para transformações no-op.
    pub fn apply<'a>(&self, s: &'a str) -> Cow<'a, str> {
        match self {
            Operation::Trim => {
                let t = s.trim();
                if t.len() == s.len() {
                    Cow::Borrowed(s)
                } else {
                    Cow::Borrowed(t)
                }
            }
            Operation::DigitsOnly => {
                if s.bytes().all(|b| b.is_ascii_digit()) {
                    Cow::Borrowed(s)
                } else {
                    Cow::Owned(s.chars().filter(|c| c.is_ascii_digit()).collect())
                }
            }
            Operation::Uppercase => {
                if s.bytes().all(|b| !b.is_ascii_lowercase()) && s.is_ascii() {
                    Cow::Borrowed(s)
                } else {
                    Cow::Owned(s.to_uppercase())
                }
            }
            Operation::Lowercase => {
                if s.bytes().all(|b| !b.is_ascii_uppercase()) && s.is_ascii() {
                    Cow::Borrowed(s)
                } else {
                    Cow::Owned(s.to_lowercase())
                }
            }
            Operation::PadLeft(len, ch) => {
                let char_count = s.chars().count();
                if char_count >= *len {
                    Cow::Borrowed(s)
                } else {
                    let needed = len - char_count;
                    let mut out = String::with_capacity(needed * ch.len_utf8() + s.len());
                    for _ in 0..needed {
                        out.push(*ch);
                    }
                    out.push_str(s);
                    Cow::Owned(out)
                }
            }
            Operation::StripDdi(ddi) => {
                if s.starts_with(ddi.as_str()) && s.len() > ddi.len() {
                    Cow::Borrowed(&s[ddi.len()..])
                } else {
                    Cow::Borrowed(s)
                }
            }
            Operation::RemoveLeadingZeroes => {
                let trimmed = s.trim_start_matches('0');
                if trimmed.len() == s.len() {
                    Cow::Borrowed(s)
                } else {
                    Cow::Borrowed(trimmed)
                }
            }
            Operation::CpfCanonical => {
                let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
                let stripped = digits.trim_start_matches('0');
                let canonical = to_n_digits(stripped, 11);
                if validate_cpf(&canonical) {
                    Cow::Owned(canonical)
                } else {
                    Cow::Owned(String::new())
                }
            }
            Operation::CnpjCanonical => {
                let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
                let stripped = digits.trim_start_matches('0');
                let canonical = to_n_digits(stripped, 14);
                if validate_cnpj(&canonical) {
                    Cow::Owned(canonical)
                } else {
                    Cow::Owned(String::new())
                }
            }
            Operation::DocumentCanonical => {
                let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
                let stripped = digits.trim_start_matches('0');

                if stripped.len() > 11 {
                    // >11 dígitos após strip: path direto CNPJ
                    let canonical = to_n_digits(stripped, 14);
                    if validate_cnpj(&canonical) {
                        return Cow::Owned(canonical);
                    }
                    return Cow::Owned(String::new());
                }

                // Primeiro tenta CPF
                let cpf = to_n_digits(stripped, 11);
                if validate_cpf(&cpf) {
                    return Cow::Owned(cpf);
                }
                // Fallback CNPJ
                let cnpj = to_n_digits(stripped, 14);
                if validate_cnpj(&cnpj) {
                    return Cow::Owned(cnpj);
                }
                Cow::Owned(String::new())
            }
            Operation::Constant(value) => Cow::Owned(value.clone()),
        }
    }
}

/// Encadeia `Operation::apply` preservando o lifetime `'a` da `Cow` de entrada.
///
/// `op.apply(&input)` pode retornar três formas de Cow:
///   1. `Borrowed(b)` onde b aponta para todo o input → no-op, reaproveita `input`.
///   2. `Borrowed(b)` onde b é subslice de input → precisa ser convertido em
///      `Owned` porque o subslice tem lifetime amarrado ao stack, não a `'a`.
///   3. `Owned(s)` → move direto.
pub(crate) fn apply_op<'a>(op: &Operation, input: Cow<'a, str>) -> Cow<'a, str> {
    let (is_identity, owned) = {
        let result = op.apply(&input);
        match result {
            Cow::Borrowed(b) => {
                let input_ref: &str = &input;
                if b.as_ptr() == input_ref.as_ptr() && b.len() == input_ref.len() {
                    (true, None)
                } else {
                    (false, Some(b.to_string()))
                }
            }
            Cow::Owned(s) => (false, Some(s)),
        }
    };
    if is_identity {
        input
    } else {
        Cow::Owned(owned.unwrap())
    }
}

pub fn parse_op(spec: &str) -> Result<Operation, String> {
    let parts: Vec<&str> = spec.splitn(3, ':').collect();
    match parts[0] {
        "trim" => Ok(Operation::Trim),
        "digits_only" => Ok(Operation::DigitsOnly),
        "uppercase" => Ok(Operation::Uppercase),
        "lowercase" => Ok(Operation::Lowercase),
        "pad_left" => {
            let len_str = parts.get(1).ok_or_else(|| {
                "pad_left requires length: 'pad_left:<len>:<char>'".to_string()
            })?;
            let len = len_str.parse().map_err(|_| {
                format!(
                    "pad_left length must be a non-negative integer, got '{}'",
                    len_str
                )
            })?;
            let ch_str = parts.get(2).ok_or_else(|| {
                "pad_left requires pad char: 'pad_left:<len>:<char>'".to_string()
            })?;
            let ch = ch_str
                .chars()
                .next()
                .ok_or_else(|| "pad_left pad char cannot be empty".to_string())?;
            Ok(Operation::PadLeft(len, ch))
        }
        "strip_ddi" => {
            let ddi = parts
                .get(1)
                .ok_or_else(|| "strip_ddi requires prefix: 'strip_ddi:<ddi>'".to_string())?;
            Ok(Operation::StripDdi(ddi.to_string()))
        }
        "remove_leading_zeroes" => Ok(Operation::RemoveLeadingZeroes),
        "cpf_canonical" => Ok(Operation::CpfCanonical),
        "cnpj_canonical" => Ok(Operation::CnpjCanonical),
        "document_canonical" => Ok(Operation::DocumentCanonical),
        "constant" => {
            // Tudo após o primeiro `:` é o valor literal — aceita `:` e
            // outros separadores dentro do valor.
            let value = spec.strip_prefix("constant:").ok_or_else(|| {
                "constant requires value: 'constant:<value>'".to_string()
            })?;
            Ok(Operation::Constant(value.to_string()))
        }
        other => Err(format!(
            "unknown operation '{}'; expected one of: trim, digits_only, uppercase, lowercase, pad_left, strip_ddi, remove_leading_zeroes, cpf_canonical, cnpj_canonical, document_canonical, constant",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_trim_removes_surrounding_whitespace() {
        assert_eq!(Operation::Trim.apply("  hello  "), "hello");
        assert_eq!(Operation::Trim.apply("\t\nfoo\n "), "foo");
    }

    #[test]
    fn op_trim_returns_borrowed_when_already_trimmed() {
        let input = "hello";
        let result = Operation::Trim.apply(input);
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ptr(), input.as_ptr());
    }

    #[test]
    fn op_uppercase_returns_borrowed_when_already_upper_ascii() {
        let input = "ABC123";
        let result = Operation::Uppercase.apply(input);
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ptr(), input.as_ptr());
    }

    #[test]
    fn op_lowercase_returns_borrowed_when_already_lower_ascii() {
        let input = "abc123";
        let result = Operation::Lowercase.apply(input);
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn op_digits_only_returns_borrowed_when_already_digits() {
        let input = "12345";
        let result = Operation::DigitsOnly.apply(input);
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn op_strip_ddi_returns_borrowed_slice_when_prefix_matches() {
        let input = "5511987654321";
        let result = Operation::StripDdi("55".into()).apply(input);
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(&*result, "11987654321");
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
        assert_eq!(Operation::StripDdi("55".into()).apply("55"), "55");
    }

    #[test]
    fn parse_op_trim() {
        assert!(matches!(parse_op("trim"), Ok(Operation::Trim)));
    }

    #[test]
    fn parse_op_digits_only() {
        assert!(matches!(parse_op("digits_only"), Ok(Operation::DigitsOnly)));
    }

    #[test]
    fn parse_op_uppercase_lowercase() {
        assert!(matches!(parse_op("uppercase"), Ok(Operation::Uppercase)));
        assert!(matches!(parse_op("lowercase"), Ok(Operation::Lowercase)));
    }

    #[test]
    fn parse_op_pad_left_with_args() {
        match parse_op("pad_left:11:0") {
            Ok(Operation::PadLeft(len, ch)) => {
                assert_eq!(len, 11);
                assert_eq!(ch, '0');
            }
            _ => panic!("expected PadLeft"),
        }
    }

    #[test]
    fn parse_op_pad_left_missing_args_returns_err() {
        assert!(parse_op("pad_left").is_err());
        assert!(parse_op("pad_left:notanumber:0").is_err());
    }

    #[test]
    fn parse_op_strip_ddi_with_value() {
        match parse_op("strip_ddi:55") {
            Ok(Operation::StripDdi(ddi)) => assert_eq!(ddi, "55"),
            _ => panic!("expected StripDdi"),
        }
    }

    #[test]
    fn parse_op_unknown_returns_err() {
        let err = parse_op("trimm").unwrap_err();
        assert!(err.contains("unknown operation"));
        assert!(err.contains("trimm"));
        assert!(parse_op("").is_err());
    }

    // ===========================================================
    // remove_leading_zeroes
    // ===========================================================

    #[test]
    fn remove_leading_zeroes_strips_prefix() {
        assert_eq!(Operation::RemoveLeadingZeroes.apply("0001234"), "1234");
        assert_eq!(Operation::RemoveLeadingZeroes.apply("0"), "");
        assert_eq!(Operation::RemoveLeadingZeroes.apply("0000"), "");
    }

    #[test]
    fn remove_leading_zeroes_noop_when_no_prefix() {
        let input = "1234";
        let result = Operation::RemoveLeadingZeroes.apply(input);
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ptr(), input.as_ptr());
    }

    #[test]
    fn remove_leading_zeroes_keeps_zeros_in_middle() {
        assert_eq!(Operation::RemoveLeadingZeroes.apply("10203"), "10203");
    }

    #[test]
    fn parse_op_remove_leading_zeroes() {
        assert!(matches!(
            parse_op("remove_leading_zeroes"),
            Ok(Operation::RemoveLeadingZeroes)
        ));
    }

    // ===========================================================
    // cpf_canonical
    // ===========================================================

    #[test]
    fn cpf_canonical_accepts_valid_plain() {
        // 12345678909 é CPF matematicamente válido
        assert_eq!(Operation::CpfCanonical.apply("12345678909"), "12345678909");
    }

    #[test]
    fn cpf_canonical_strips_non_digits() {
        assert_eq!(
            Operation::CpfCanonical.apply("123.456.789-09"),
            "12345678909"
        );
    }

    #[test]
    fn cpf_canonical_strips_leading_zeros_and_repads() {
        // PHP pipe: digits|remove_leading_zeroes|right:11|pad_left:0,11
        // "0000012345678909" (15 digits w/ leading zeros)
        // → strip → "12345678909" → right:11 → "12345678909" → pad → same
        assert_eq!(
            Operation::CpfCanonical.apply("0000012345678909"),
            "12345678909"
        );
    }

    #[test]
    fn cpf_canonical_returns_empty_for_invalid() {
        assert_eq!(Operation::CpfCanonical.apply("11111111111"), "");
        assert_eq!(Operation::CpfCanonical.apply("99"), "");
        assert_eq!(Operation::CpfCanonical.apply(""), "");
        assert_eq!(Operation::CpfCanonical.apply("abc"), "");
    }

    #[test]
    fn parse_op_cpf_canonical() {
        assert!(matches!(parse_op("cpf_canonical"), Ok(Operation::CpfCanonical)));
    }

    // ===========================================================
    // cnpj_canonical
    // ===========================================================

    #[test]
    fn cnpj_canonical_accepts_valid() {
        assert_eq!(
            Operation::CnpjCanonical.apply("11222333000181"),
            "11222333000181"
        );
    }

    #[test]
    fn cnpj_canonical_strips_formatting() {
        assert_eq!(
            Operation::CnpjCanonical.apply("11.222.333/0001-81"),
            "11222333000181"
        );
    }

    #[test]
    fn cnpj_canonical_returns_empty_for_invalid() {
        assert_eq!(Operation::CnpjCanonical.apply("11111111111111"), "");
        assert_eq!(Operation::CnpjCanonical.apply("12345"), "");
    }

    #[test]
    fn parse_op_cnpj_canonical() {
        assert!(matches!(
            parse_op("cnpj_canonical"),
            Ok(Operation::CnpjCanonical)
        ));
    }

    // ===========================================================
    // document_canonical
    // ===========================================================

    #[test]
    fn document_canonical_accepts_valid_cpf() {
        assert_eq!(
            Operation::DocumentCanonical.apply("12345678909"),
            "12345678909"
        );
        // formatado
        assert_eq!(
            Operation::DocumentCanonical.apply("123.456.789-09"),
            "12345678909"
        );
    }

    #[test]
    fn document_canonical_accepts_valid_cnpj() {
        assert_eq!(
            Operation::DocumentCanonical.apply("11222333000181"),
            "11222333000181"
        );
        assert_eq!(
            Operation::DocumentCanonical.apply("11.222.333/0001-81"),
            "11222333000181"
        );
    }

    #[test]
    fn document_canonical_empty_when_neither_valid() {
        assert_eq!(Operation::DocumentCanonical.apply(""), "");
        assert_eq!(Operation::DocumentCanonical.apply("abc"), "");
        assert_eq!(Operation::DocumentCanonical.apply("12345"), "");
        // sequências inválidas
        assert_eq!(Operation::DocumentCanonical.apply("11111111111"), "");
        assert_eq!(Operation::DocumentCanonical.apply("11111111111111"), "");
    }

    #[test]
    fn document_canonical_handles_leading_zeros() {
        // CPF válido com zeros à esquerda preservados no input
        assert_eq!(
            Operation::DocumentCanonical.apply("0012345678909"),
            "12345678909"
        );
    }

    #[test]
    fn parse_op_document_canonical() {
        assert!(matches!(
            parse_op("document_canonical"),
            Ok(Operation::DocumentCanonical)
        ));
    }

    // ===========================================================
    // constant
    // ===========================================================

    #[test]
    fn constant_emits_fixed_value_ignoring_input() {
        let op = Operation::Constant("tenant_abc".to_string());
        // Input value is ignored — always emits the literal.
        assert_eq!(op.apply("anything"), "tenant_abc");
        assert_eq!(op.apply(""), "tenant_abc");
        assert_eq!(op.apply("33176825404"), "tenant_abc");
    }

    #[test]
    fn constant_empty_value_emits_empty() {
        // `constant:` (nothing after colon) produces empty string,
        // effectively a `blank` op.
        let op = Operation::Constant(String::new());
        assert_eq!(op.apply("whatever"), "");
    }

    #[test]
    fn parse_op_constant_simple_value() {
        match parse_op("constant:tenant_abc") {
            Ok(Operation::Constant(v)) => assert_eq!(v, "tenant_abc"),
            other => panic!("expected Constant, got {:?}", other),
        }
    }

    #[test]
    fn parse_op_constant_numeric_value() {
        match parse_op("constant:42") {
            Ok(Operation::Constant(v)) => assert_eq!(v, "42"),
            _ => panic!("expected Constant"),
        }
    }

    #[test]
    fn parse_op_constant_empty_value() {
        // `constant:` (nothing after colon) → Constant("")
        match parse_op("constant:") {
            Ok(Operation::Constant(v)) => assert_eq!(v, ""),
            _ => panic!("expected Constant with empty string"),
        }
    }

    #[test]
    fn parse_op_constant_preserves_colons_in_value() {
        // Everything after the FIRST `:` is part of the value.
        // Important for values like timestamps or URLs.
        match parse_op("constant:2026-04-24T10:30:00") {
            Ok(Operation::Constant(v)) => assert_eq!(v, "2026-04-24T10:30:00"),
            _ => panic!("expected Constant"),
        }
    }

    #[test]
    fn parse_op_constant_without_colon_errors() {
        // Bare `constant` with no colon is an error — explicit prefix required.
        let err = parse_op("constant").unwrap_err();
        assert!(err.contains("constant"));
        assert!(err.contains("requires value"));
    }

    #[test]
    fn parse_op_constant_chained_in_layout_pattern() {
        // Simulates how a layout would use it: constant chained with nothing
        // else. The op should work as the sole op in a column's pipeline.
        let op = parse_op("constant:job_42").unwrap();
        let input_val = "ignore_me";
        let result = op.apply(input_val);
        assert_eq!(result, "job_42");
    }
}
