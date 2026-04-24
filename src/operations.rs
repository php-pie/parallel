use std::borrow::Cow;

#[derive(Clone, Debug)]
pub enum Operation {
    Trim,
    DigitsOnly,
    Uppercase,
    Lowercase,
    PadLeft(usize, char),
    StripDdi(String),
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
        other => Err(format!(
            "unknown operation '{}'; expected one of: trim, digits_only, uppercase, lowercase, pad_left, strip_ddi",
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
}
