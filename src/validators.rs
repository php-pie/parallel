#[derive(Clone, Debug)]
pub enum Validator {
    Cpf,
    PhoneBr,
    Cnpj,
    Email,
    Length(usize, usize),
    Regex(regex::Regex),
    /// Campo não pode estar em branco. Combina com ops canônicos que
    /// retornam `""` em caso de falha (ex.: `document_canonical`) para
    /// dropar a linha inteira quando o campo-chave ficar vazio.
    NotBlank,
}

impl Validator {
    pub fn check(&self, s: &str) -> bool {
        match self {
            Validator::Cpf => validate_cpf(s),
            Validator::PhoneBr => validate_phone_br(s),
            Validator::Cnpj => validate_cnpj(s),
            Validator::Email => validate_email(s),
            Validator::Length(min, max) => {
                let count = s.chars().count();
                count >= *min && count <= *max
            }
            Validator::Regex(re) => re.is_match(s),
            Validator::NotBlank => !s.is_empty(),
        }
    }
}

pub fn parse_validator(spec: &str) -> Result<Validator, String> {
    let (name, rest) = match spec.find(':') {
        Some(idx) => (&spec[..idx], Some(&spec[idx + 1..])),
        None => (spec, None),
    };
    match name {
        "cpf" => Ok(Validator::Cpf),
        "phone_br" => Ok(Validator::PhoneBr),
        "cnpj" => Ok(Validator::Cnpj),
        "email" => Ok(Validator::Email),
        "not_blank" => Ok(Validator::NotBlank),
        "length" => {
            let args = rest.ok_or_else(|| {
                "length requires args: 'length:<min>:<max>'".to_string()
            })?;
            let (min_s, max_s) = args.split_once(':').ok_or_else(|| {
                "length requires both min and max: 'length:<min>:<max>'".to_string()
            })?;
            let min: usize = min_s.parse().map_err(|_| {
                format!("length min must be a non-negative integer, got '{}'", min_s)
            })?;
            let max: usize = max_s.parse().map_err(|_| {
                format!("length max must be a non-negative integer, got '{}'", max_s)
            })?;
            if min > max {
                return Err(format!("length min ({}) must be <= max ({})", min, max));
            }
            Ok(Validator::Length(min, max))
        }
        "regex" => {
            let pattern = rest.ok_or_else(|| {
                "regex requires a pattern: 'regex:<pattern>'".to_string()
            })?;
            let re = regex::Regex::new(pattern)
                .map_err(|e| format!("invalid regex '{}': {}", pattern, e))?;
            Ok(Validator::Regex(re))
        }
        other => Err(format!(
            "unknown validator '{}'; expected one of: cpf, phone_br, cnpj, email, length, regex, not_blank",
            other
        )),
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

pub fn validate_cnpj(s: &str) -> bool {
    let digits: Vec<u8> = s
        .bytes()
        .filter(|b| b.is_ascii_digit())
        .map(|b| b - b'0')
        .collect();
    if digits.len() != 14 {
        return false;
    }
    if digits.iter().all(|&d| d == digits[0]) {
        return false;
    }
    // primeiro dígito verificador
    let mult1: [u32; 12] = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
    let sum: u32 = (0..12).map(|i| digits[i] as u32 * mult1[i]).sum();
    let r = sum % 11;
    let d1: u8 = if r < 2 { 0 } else { (11 - r) as u8 };
    if d1 != digits[12] {
        return false;
    }
    // segundo dígito verificador
    let mult2: [u32; 13] = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
    let sum: u32 = (0..13).map(|i| digits[i] as u32 * mult2[i]).sum();
    let r = sum % 11;
    let d2: u8 = if r < 2 { 0 } else { (11 - r) as u8 };
    d2 == digits[13]
}

/// Validação heurística de email: local@dominio com ponto no domínio e sem
/// espaços. Não pretende cobrir RFC 5322 — é o nível de rigor típico em ETL
/// para separar valores obviamente inválidos ("", "foo", "foo@", "@bar.com")
/// de plausivelmente válidos.
pub fn validate_email(s: &str) -> bool {
    if s.contains(char::is_whitespace) {
        return false;
    }
    let (local, domain) = match s.split_once('@') {
        Some(parts) => parts,
        None => return false,
    };
    if local.is_empty() || domain.is_empty() {
        return false;
    }
    if domain.contains('@') {
        return false;
    }
    if !domain.contains('.') {
        return false;
    }
    if domain.starts_with('.') || domain.ends_with('.') {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===========================================================
    // validate_cpf
    // ===========================================================

    #[test]
    fn cpf_accepts_valid_cpf_plain_digits() {
        assert!(validate_cpf("12345678909"));
    }

    #[test]
    fn cpf_accepts_valid_cpf_with_formatting() {
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
    // validate_cnpj
    // ===========================================================

    #[test]
    fn cnpj_accepts_valid_plain_digits() {
        assert!(validate_cnpj("11222333000181"));
    }

    #[test]
    fn cnpj_accepts_valid_with_formatting() {
        assert!(validate_cnpj("11.222.333/0001-81"));
    }

    #[test]
    fn cnpj_rejects_wrong_length() {
        assert!(!validate_cnpj(""));
        assert!(!validate_cnpj("11222333000"));
        assert!(!validate_cnpj("112223330001812"));
    }

    #[test]
    fn cnpj_rejects_all_same_digits() {
        assert!(!validate_cnpj("00000000000000"));
        assert!(!validate_cnpj("11111111111111"));
    }

    #[test]
    fn cnpj_rejects_wrong_check_digits() {
        assert!(!validate_cnpj("11222333000182"));
        assert!(!validate_cnpj("11222333000191"));
    }

    // ===========================================================
    // validate_email
    // ===========================================================

    #[test]
    fn email_accepts_plain_address() {
        assert!(validate_email("foo@bar.com"));
        assert!(validate_email("a.b+tag@sub.example.co.uk"));
    }

    #[test]
    fn email_rejects_missing_at_or_domain() {
        assert!(!validate_email(""));
        assert!(!validate_email("foo"));
        assert!(!validate_email("@bar.com"));
        assert!(!validate_email("foo@"));
    }

    #[test]
    fn email_rejects_domain_without_dot() {
        assert!(!validate_email("foo@bar"));
    }

    #[test]
    fn email_rejects_whitespace() {
        assert!(!validate_email("foo @bar.com"));
        assert!(!validate_email("foo@bar .com"));
    }

    #[test]
    fn email_rejects_domain_with_leading_or_trailing_dot() {
        assert!(!validate_email("foo@.bar.com"));
        assert!(!validate_email("foo@bar.com."));
    }

    #[test]
    fn email_rejects_multiple_at() {
        assert!(!validate_email("foo@bar@baz.com"));
    }

    // ===========================================================
    // Validator::check (integração)
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
    // Validator::Length
    // ===========================================================

    #[test]
    fn length_accepts_within_range() {
        assert!(Validator::Length(3, 5).check("abc"));
        assert!(Validator::Length(3, 5).check("abcd"));
        assert!(Validator::Length(3, 5).check("abcde"));
    }

    #[test]
    fn length_rejects_out_of_range() {
        assert!(!Validator::Length(3, 5).check("ab"));
        assert!(!Validator::Length(3, 5).check("abcdef"));
    }

    #[test]
    fn length_counts_chars_not_bytes() {
        assert!(Validator::Length(5, 5).check("áéíóú"));
        assert!(!Validator::Length(10, 10).check("áéíóú"));
    }

    // ===========================================================
    // Validator::Regex
    // ===========================================================

    #[test]
    fn regex_matches_pattern() {
        let re = regex::Regex::new(r"^\d{5}-\d{3}$").unwrap();
        let v = Validator::Regex(re);
        assert!(v.check("01310-100"));
        assert!(!v.check("01310100"));
        assert!(!v.check("abcde-fgh"));
    }

    // ===========================================================
    // parse_validator
    // ===========================================================

    #[test]
    fn parse_validator_bare_names() {
        assert!(matches!(parse_validator("cpf"), Ok(Validator::Cpf)));
        assert!(matches!(parse_validator("phone_br"), Ok(Validator::PhoneBr)));
        assert!(matches!(parse_validator("cnpj"), Ok(Validator::Cnpj)));
        assert!(matches!(parse_validator("email"), Ok(Validator::Email)));
    }

    #[test]
    fn parse_validator_length_with_args() {
        match parse_validator("length:3:50") {
            Ok(Validator::Length(min, max)) => {
                assert_eq!(min, 3);
                assert_eq!(max, 50);
            }
            _ => panic!("expected Length"),
        }
    }

    #[test]
    fn parse_validator_length_rejects_missing_args() {
        assert!(parse_validator("length").is_err());
        assert!(parse_validator("length:3").is_err());
        assert!(parse_validator("length:abc:5").is_err());
    }

    #[test]
    fn parse_validator_length_rejects_min_gt_max() {
        let err = parse_validator("length:10:5").unwrap_err();
        assert!(err.contains("min") && err.contains("max"));
    }

    #[test]
    fn parse_validator_regex_with_pattern() {
        match parse_validator(r"regex:^[A-Z]{2}\d{4}$") {
            Ok(Validator::Regex(re)) => {
                assert!(re.is_match("AB1234"));
                assert!(!re.is_match("ab1234"));
            }
            _ => panic!("expected Regex"),
        }
    }

    #[test]
    fn parse_validator_regex_rejects_missing_pattern() {
        assert!(parse_validator("regex").is_err());
    }

    #[test]
    fn parse_validator_regex_rejects_invalid_pattern() {
        let err = parse_validator("regex:[unclosed").unwrap_err();
        assert!(err.to_lowercase().contains("regex"));
    }

    #[test]
    fn parse_validator_unknown() {
        let err = parse_validator("passport").unwrap_err();
        assert!(err.contains("unknown validator"));
    }

    // ===========================================================
    // Validator::NotBlank
    // ===========================================================

    #[test]
    fn not_blank_rejects_empty_string() {
        assert!(!Validator::NotBlank.check(""));
    }

    #[test]
    fn not_blank_accepts_non_empty() {
        assert!(Validator::NotBlank.check("x"));
        assert!(Validator::NotBlank.check(" "));
        assert!(Validator::NotBlank.check("12345678909"));
    }

    #[test]
    fn parse_validator_not_blank() {
        assert!(matches!(parse_validator("not_blank"), Ok(Validator::NotBlank)));
    }
}
