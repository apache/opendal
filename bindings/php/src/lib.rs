use ext_php_rs::prelude::*;
use opendal::{EntryMode, Metadata};

#[php_function]
pub fn debug() -> String {
    let metadata = Metadata::new(EntryMode::FILE);

    format!("{:?}", metadata)
}

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
}
