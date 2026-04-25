#[derive(Clone, Debug)]
pub enum Validator {
    /// CPF brasileiro (11 dígitos + dígitos verificadores).
    Cpf,
    /// CNPJ brasileiro (14 dígitos + dígitos verificadores).
    Cnpj,
    /// CPF OU CNPJ — aceita qualquer um dos dois. Útil para o campo
    /// `document` que pode ser qualquer dos dois tipos.
    Document,
    /// DDD brasileiro (2 dígitos, lista oficial de áreas válidas).
    AreaCode,
    /// Número de telefone brasileiro SEM DDD (assinante).
    /// Fixo: 8 dígitos começando com 2-8. Celular: 9 dígitos começando com 9.
    Phone,
    /// Email (heurística: `local@dominio` com ponto no domínio).
    Email,
    /// Regex arbitrária (sintaxe da crate `regex`).
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
            Validator::Cnpj => validate_cnpj(s),
            Validator::Document => validate_document(s),
            Validator::AreaCode => validate_area_code(s),
            Validator::Phone => validate_phone(s),
            Validator::Email => validate_email(s),
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
        "cnpj" => Ok(Validator::Cnpj),
        "document" => Ok(Validator::Document),
        "area_code" => Ok(Validator::AreaCode),
        "phone" => Ok(Validator::Phone),
        "email" => Ok(Validator::Email),
        "not_blank" => Ok(Validator::NotBlank),
        "regex" => {
            let pattern = rest.ok_or_else(|| {
                "regex requires a pattern: 'regex:<pattern>'".to_string()
            })?;
            let re = regex::Regex::new(pattern)
                .map_err(|e| format!("invalid regex '{}': {}", pattern, e))?;
            Ok(Validator::Regex(re))
        }
        other => Err(format!(
            "unknown validator '{}'; expected one of: cpf, cnpj, document, area_code, phone, email, regex, not_blank",
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

/// Telefone brasileiro **sem DDD** (assinante). Alinha com o
/// `sanitizePhoneNumber` do DataSanitizer.
///
/// - Fixo: 8 dígitos, primeiro dígito ∈ 2-8 (`[2-8]\d{7}`)
/// - Celular: 9 dígitos, primeiro dígito é `9` (`9\d{8}`)
///
/// Para o número completo com DDD, use duas colunas: uma com validador
/// `area_code` (os 2 dígitos do DDD) e outra com `phone` (o assinante).
pub fn validate_phone(s: &str) -> bool {
    let digits: Vec<u8> = s.bytes().filter(|b| b.is_ascii_digit()).collect();
    match digits.len() {
        8 => matches!(digits[0], b'2'..=b'8'),
        9 => digits[0] == b'9',
        _ => false,
    }
}

/// Valida se uma string é CPF **ou** CNPJ. Útil para o campo `document`
/// que pode ser qualquer um dos dois tipos.
pub fn validate_document(s: &str) -> bool {
    validate_cpf(s) || validate_cnpj(s)
}

/// DDD brasileiro (2 dígitos). Lista oficial de áreas:
///
/// - `11-19` (SP), `21 22 24 27 28` (RJ/ES), `31-35 37 38` (MG),
///   `41-49` (Sul), `51 53 54 55` (RS), `61-69` (DF/GO/TO/MT/MS/AC/RO),
///   `71 73 74 75 77 79` (BA/SE), `81-89` (NE), `91-99` (N)
pub fn validate_area_code(s: &str) -> bool {
    let digits: Vec<u8> = s.bytes().filter(|b| b.is_ascii_digit()).collect();
    if digits.len() != 2 {
        return false;
    }
    let d0 = digits[0];
    let d1 = digits[1];
    match d0 {
        b'1' | b'4' | b'6' | b'8' | b'9' => matches!(d1, b'1'..=b'9'),
        b'2' => matches!(d1, b'1' | b'2' | b'4' | b'7' | b'8'),
        b'3' => matches!(d1, b'1'..=b'5' | b'7' | b'8'),
        b'5' => matches!(d1, b'1' | b'3' | b'4' | b'5'),
        b'7' => matches!(d1, b'1' | b'3' | b'4' | b'5' | b'7' | b'9'),
        _ => false,
    }
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
    // validate_phone (sem DDD — assinante apenas)
    // ===========================================================

    #[test]
    fn phone_accepts_landline_8_digits() {
        assert!(validate_phone("22334455")); // fixo começando com 2
        assert!(validate_phone("88887777")); // fixo começando com 8
    }

    #[test]
    fn phone_accepts_cellphone_9_digits_starting_with_9() {
        assert!(validate_phone("987654321"));
        assert!(validate_phone("999998888"));
    }

    #[test]
    fn phone_rejects_landline_starting_with_9() {
        // 8 dígitos começando com 9 é inválido (celular precisa ter 9 dígitos)
        assert!(!validate_phone("92345678"));
    }

    #[test]
    fn phone_rejects_landline_starting_with_1() {
        assert!(!validate_phone("12345678"));
    }

    #[test]
    fn phone_rejects_cellphone_without_leading_9() {
        assert!(!validate_phone("887654321"));
    }

    #[test]
    fn phone_rejects_wrong_length() {
        assert!(!validate_phone(""));
        assert!(!validate_phone("123"));
        assert!(!validate_phone("1234567"));      // 7
        assert!(!validate_phone("1234567890"));   // 10
    }

    #[test]
    fn phone_strips_non_digits_before_validating() {
        assert!(validate_phone("98765-4321"));
    }

    // ===========================================================
    // validate_document (CPF ou CNPJ)
    // ===========================================================

    #[test]
    fn document_accepts_valid_cpf() {
        assert!(validate_document("12345678909"));
        assert!(validate_document("123.456.789-09"));
    }

    #[test]
    fn document_accepts_valid_cnpj() {
        assert!(validate_document("11222333000181"));
        assert!(validate_document("11.222.333/0001-81"));
    }

    #[test]
    fn document_rejects_invalid() {
        assert!(!validate_document(""));
        assert!(!validate_document("11111111111"));
        assert!(!validate_document("11111111111111"));
        assert!(!validate_document("12345"));
    }

    // ===========================================================
    // validate_area_code
    // ===========================================================

    #[test]
    fn area_code_accepts_valid_ddds() {
        // SP
        for cc in ["11", "12", "13", "14", "15", "16", "17", "18", "19"] {
            assert!(validate_area_code(cc), "expected {} to be valid", cc);
        }
        // RJ/ES
        for cc in ["21", "22", "24", "27", "28"] {
            assert!(validate_area_code(cc), "expected {} to be valid", cc);
        }
        // MG
        for cc in ["31", "32", "33", "34", "35", "37", "38"] {
            assert!(validate_area_code(cc), "expected {} to be valid", cc);
        }
        // Sul
        for cc in ["41", "42", "43", "44", "45", "46", "47", "48", "49"] {
            assert!(validate_area_code(cc), "expected {} to be valid", cc);
        }
        // RS
        for cc in ["51", "53", "54", "55"] {
            assert!(validate_area_code(cc), "expected {} to be valid", cc);
        }
        // Centro-Oeste / N
        for cc in ["61", "62", "63", "64", "65", "66", "67", "68", "69"] {
            assert!(validate_area_code(cc), "expected {} to be valid", cc);
        }
        // BA/SE
        for cc in ["71", "73", "74", "75", "77", "79"] {
            assert!(validate_area_code(cc), "expected {} to be valid", cc);
        }
        // NE / N
        for cc in ["81", "91", "99"] {
            assert!(validate_area_code(cc), "expected {} to be valid", cc);
        }
    }

    #[test]
    fn area_code_rejects_holes_in_list() {
        // Números que não são DDDs válidos
        for cc in ["10", "20", "23", "25", "26", "29", "30", "36", "39", "40",
                   "50", "52", "56", "57", "58", "59", "60", "70", "72", "76", "78",
                   "80", "90"] {
            assert!(!validate_area_code(cc), "expected {} to be invalid", cc);
        }
    }

    #[test]
    fn area_code_rejects_wrong_length() {
        assert!(!validate_area_code(""));
        assert!(!validate_area_code("1"));
        assert!(!validate_area_code("111"));
    }

    #[test]
    fn area_code_strips_non_digits() {
        assert!(validate_area_code("(11)"));
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
    fn validator_cnpj_delegates_to_validate_cnpj() {
        assert!(Validator::Cnpj.check("11222333000181"));
        assert!(!Validator::Cnpj.check("11111111111111"));
    }

    #[test]
    fn validator_document_delegates_to_validate_document() {
        assert!(Validator::Document.check("12345678909"));      // CPF
        assert!(Validator::Document.check("11222333000181"));   // CNPJ
        assert!(!Validator::Document.check("11111111111"));
    }

    #[test]
    fn validator_phone_delegates_to_validate_phone() {
        assert!(Validator::Phone.check("987654321"));
        assert!(!Validator::Phone.check("12345678"));
    }

    #[test]
    fn validator_area_code_delegates_to_validate_area_code() {
        assert!(Validator::AreaCode.check("11"));
        assert!(!Validator::AreaCode.check("10"));
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
        assert!(matches!(parse_validator("cnpj"), Ok(Validator::Cnpj)));
        assert!(matches!(parse_validator("document"), Ok(Validator::Document)));
        assert!(matches!(parse_validator("area_code"), Ok(Validator::AreaCode)));
        assert!(matches!(parse_validator("phone"), Ok(Validator::Phone)));
        assert!(matches!(parse_validator("email"), Ok(Validator::Email)));
    }

    #[test]
    fn parse_validator_old_phone_br_rejected() {
        // phone_br foi renomeado para phone
        let err = parse_validator("phone_br").unwrap_err();
        assert!(err.contains("unknown validator"));
    }

    #[test]
    fn parse_validator_old_length_rejected() {
        // length foi removido — use regex:^.{min,max}$ se precisar
        let err = parse_validator("length:3:50").unwrap_err();
        assert!(err.contains("unknown validator"));
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
