use std::fmt;
use madsim::net::rpc::Deserialize;
use chrono::prelude::*;

use crate::raw::*;
use crate::*;

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxErrorResponse {
    pub error_summary: String,
    pub error: DropboxErrorDetail,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
struct DropboxErrorDetail {
    #[serde(rename(deserialize = ".tag"))]
    pub tag: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataResponse {
    #[serde(rename(deserialize = ".tag"))]
    pub tag: String,
    pub client_modified: String,
    pub content_hash: Option<String>,
    pub file_lock_info: Option<DropboxMetadataFileLockInfo>,
    pub has_explicit_shared_members: Option<bool>,
    pub id: String,
    pub is_downloadable: Option<bool>,
    pub name: String,
    pub path_display: String,
    pub path_lower: String,
    pub property_groups: Option<Vec<DropboxMetadataPropertyGroup>>,
    pub rev: Option<String>,
    pub server_modified: Option<String>,
    pub sharing_info: Option<DropboxMetadataSharingInfo>,
    pub size: Option<u64>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataFileLockInfo {
    pub created: Option<String>,
    pub is_lockholder: bool,
    pub lockholder_name: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataPropertyGroup {
    pub fields: Vec<DropboxMetadataPropertyGroupField>,
    pub template_id: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataPropertyGroupField {
    pub name: String,
    pub value: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
pub struct DropboxMetadataSharingInfo {
    pub modified_by: Option<String>,
    pub parent_shared_folder_id: Option<String>,
    pub read_only: Option<bool>,
    pub shared_folder_id: Option<String>,
    pub traverse_only: Option<bool>,
    pub no_access: Option<bool>,
}

pub enum DropboxFileType {
    File,
    Folder,
}

impl fmt::Display for DropboxFileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DropboxFileType::File => write!(f, "file"),
            DropboxFileType::Folder => write!(f, "folder"),
        }
    }
}