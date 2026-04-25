#[cfg(feature = "extension")]
use ext_php_rs::prelude::*;

pub mod layout;
pub mod operations;
pub mod processor;
pub mod validators;

// Re-exports para manter a API pública plana.
pub use layout::{parse_columns, ColumnConfig};
pub use operations::{parse_op, Operation};
pub use processor::FileProcessor;
pub use validators::{
    parse_validator, validate_area_code, validate_cnpj, validate_cpf, validate_document,
    validate_email, validate_phone, Validator,
};

#[cfg(feature = "extension")]
#[php_module]
pub fn module(module: ModuleBuilder) -> ModuleBuilder {
    module.class::<FileProcessor>()
}
