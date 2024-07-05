use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileBlob {
    pub etag: Option<String>,
    pub md5: Option<String>,
}
