use wasm_bindgen::JsValue;

use crate::Error;
use crate::ErrorKind;

impl From<JsValue> for Error {
    fn from(value: JsValue) -> Self {
        Error::new(
            ErrorKind::Unexpected,
            value.as_string().unwrap_or_else(|| "Error".to_owned()),
        )
    }
}
