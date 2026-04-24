use crate::operations::{parse_op, Operation};
use crate::validators::{parse_validator, Validator};

#[derive(Clone, Debug)]
pub struct ColumnConfig {
    pub input_index: usize,
    pub output_index: usize,
    pub ops: Vec<Operation>,
    pub validate: Option<Validator>,
}

pub fn parse_columns(json: &str) -> Result<Vec<ColumnConfig>, String> {
    let parsed: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("invalid json: {}", e))?;
    let arr = parsed.as_array().ok_or_else(|| "expected array".to_string())?;
    let mut cols = Vec::with_capacity(arr.len());
    for (idx, item) in arr.iter().enumerate() {
        let in_idx = item["in"]
            .as_u64()
            .ok_or_else(|| format!("column {}: missing or invalid 'in'", idx))?
            as usize;
        let out_idx = item["out"]
            .as_u64()
            .ok_or_else(|| format!("column {}: missing or invalid 'out'", idx))?
            as usize;
        let ops_arr = item["ops"]
            .as_array()
            .ok_or_else(|| format!("column {}: missing or invalid 'ops' (must be array)", idx))?;
        let ops: Result<Vec<Operation>, String> = ops_arr
            .iter()
            .enumerate()
            .map(|(op_idx, v)| {
                let spec = v.as_str().ok_or_else(|| {
                    format!("column {}: ops[{}] must be a string", idx, op_idx)
                })?;
                parse_op(spec).map_err(|e| format!("column {}: ops[{}]: {}", idx, op_idx, e))
            })
            .collect();
        let ops = ops?;
        let validate = match item["validate"].as_str() {
            None => None,
            Some(spec) => {
                Some(parse_validator(spec).map_err(|e| format!("column {}: {}", idx, e))?)
            }
        };
        cols.push(ColumnConfig {
            input_index: in_idx,
            output_index: out_idx,
            ops,
            validate,
        });
    }
    Ok(cols)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let json = r#"[{"in":0,"out":0,"ops":[],"validate":"phone"}]"#;
        let cols = parse_columns(json).unwrap();
        assert!(matches!(cols[0].validate, Some(Validator::Phone)));
    }

    #[test]
    fn parse_columns_with_document_validator() {
        let json = r#"[{"in":0,"out":0,"ops":[],"validate":"document"}]"#;
        let cols = parse_columns(json).unwrap();
        assert!(matches!(cols[0].validate, Some(Validator::Document)));
    }

    #[test]
    fn parse_columns_with_area_code_validator() {
        let json = r#"[{"in":0,"out":0,"ops":[],"validate":"area_code"}]"#;
        let cols = parse_columns(json).unwrap();
        assert!(matches!(cols[0].validate, Some(Validator::AreaCode)));
    }

    #[test]
    fn parse_columns_rejects_unknown_validator() {
        let json = r#"[{"in":0,"out":0,"ops":[],"validate":"passport"}]"#;
        let err = parse_columns(json).unwrap_err();
        assert!(err.contains("unknown validator"));
        assert!(err.contains("passport"));
    }

    #[test]
    fn parse_columns_rejects_unknown_ops() {
        let json = r#"[{"in":0,"out":0,"ops":["trim","not_a_real_op","uppercase"]}]"#;
        let err = parse_columns(json).unwrap_err();
        assert!(err.contains("not_a_real_op"));
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

    #[test]
    fn parse_columns_accepts_multiple_validators() {
        let json = r#"[
            {"in":0,"out":0,"ops":[],"validate":"cnpj"},
            {"in":1,"out":1,"ops":[],"validate":"email"},
            {"in":2,"out":2,"ops":[],"validate":"area_code"},
            {"in":3,"out":3,"ops":[],"validate":"phone"},
            {"in":4,"out":4,"ops":[],"validate":"document"},
            {"in":5,"out":5,"ops":[],"validate":"not_blank"},
            {"in":6,"out":6,"ops":[],"validate":"regex:^\\d+$"}
        ]"#;
        let cols = parse_columns(json).unwrap();
        assert_eq!(cols.len(), 7);
        assert!(matches!(cols[0].validate, Some(Validator::Cnpj)));
        assert!(matches!(cols[1].validate, Some(Validator::Email)));
        assert!(matches!(cols[2].validate, Some(Validator::AreaCode)));
        assert!(matches!(cols[3].validate, Some(Validator::Phone)));
        assert!(matches!(cols[4].validate, Some(Validator::Document)));
        assert!(matches!(cols[5].validate, Some(Validator::NotBlank)));
        assert!(matches!(cols[6].validate, Some(Validator::Regex(_))));
    }

    #[test]
    fn parse_columns_surfaces_validator_error_with_column_index() {
        let json = r#"[
            {"in":0,"out":0,"ops":[]},
            {"in":1,"out":1,"ops":[],"validate":"nonsense"}
        ]"#;
        let err = parse_columns(json).unwrap_err();
        assert!(err.contains("column 1"));
        assert!(err.contains("unknown validator"));
    }
}
