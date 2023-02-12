#[derive(Debug, Clone)]
pub enum BodyRequestType {
    PUT,
    PROPFIND,
}

impl ToString for BodyRequestType {
    fn to_string(&self) -> String {
        match self {
            BodyRequestType::PUT => "PUT".to_string(),
            BodyRequestType::PROPFIND => "PROPFIND".to_string(),
        }
    }
}
