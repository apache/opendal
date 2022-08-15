use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// `RawMeta` is an intermediate type able to
/// deserialize directly from JSON data.
///
/// In OpenDAL, `ObjectMetadata`'s `last_modified` field's type is `time::OffsetDateTime`,
/// which could only be represented as strings in JSON files.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RawMeta {
    pub size: u64,
    pub etag: String,
    pub last_modified: String, // rfc3339 styled datetime string
    pub md5_hash: String,
}

/// `GcsMeta` represents necessary parts of data that we need to read from GCS.
#[derive(Debug)]
pub(crate) struct GcsMeta {
    pub size: u64,
    pub etag: String,
    pub last_modified: OffsetDateTime,
    pub md5_hash: String,
}

impl TryFrom<RawMeta> for GcsMeta {
    type Error = time::error::Parse;

    fn try_from(value: RawMeta) -> Result<Self, Self::Error> {
        let last_modified = OffsetDateTime::parse(value.last_modified.as_str(), &Rfc3339)?;
        Ok(Self {
            size: value.size,
            etag: value.etag,
            last_modified,
            md5_hash: value.md5_hash,
        })
    }
}
