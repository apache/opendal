// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use anyhow::{anyhow, Context};
use itertools::Itertools;
use log::debug;
use std::collections::HashMap;
use std::fs::read_dir;
use std::str::FromStr;
use std::{fs, vec};
use syn::{Field, GenericArgument, Item, PathArguments, Type, TypePath};

#[derive(Debug, Clone)]
pub struct Services(HashMap<String, Service>);

impl IntoIterator for Services {
    type Item = (String, Service);
    type IntoIter = vec::IntoIter<(String, Service)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter().sorted_by_key(|x| x.0.clone())
    }
}

/// Service represents a service supported by opendal core, like `s3` and `fs`
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Service {
    /// All configurations for this service.
    pub config: Vec<Config>,
}

/// Config represents a configuration item for a service.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Config {
    /// The name of this config, for example, `access_key_id` and `secret_access_key`
    pub name: String,
    /// The value type this config.
    pub value: ConfigType,
    /// If given config is optional or not.
    pub optional: bool,
    /// if this field is deprecated, a deprecated message will be provided.
    pub deprecated: Option<String>,
    /// The comments for this config.
    ///
    /// All white spaces and extra new lines will be trimmed.
    pub comments: String,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ConfigType {
    /// Mapping to rust's `bool`
    Bool,
    /// Mapping to rust's `String`
    String,
    /// Mapping to rust's `Duration`
    Duration,

    /// Mapping to rust's `usize`
    Usize,
    /// Mapping to rust's `u64`
    U64,
    /// Mapping to rust's `i64`
    I64,
    /// Mapping to rust's `u32`
    U32,
    /// Mapping to rust's `u16`
    U16,

    /// Mapping to rust's `Vec`
    ///
    /// Please note, all vec in config are `,` separated string.
    Vec,
}

impl FromStr for ConfigType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let ct = match s {
            "bool" => ConfigType::Bool,
            "String" => ConfigType::String,
            "Duration" => ConfigType::Duration,

            "usize" => ConfigType::Usize,
            "u64" => ConfigType::U64,
            "i64" => ConfigType::I64,
            "u32" => ConfigType::U32,
            "u16" => ConfigType::U16,

            "Vec" => ConfigType::Vec,
            v => return Err(anyhow!("unsupported config type {v:?}")),
        };

        Ok(ct)
    }
}

/// List and parse given path to a `Services` struct.
pub fn parse(path: &str) -> Result<Services> {
    let mut map = HashMap::default();

    for dir in read_dir(path)? {
        let dir = dir?;
        if dir.file_type()?.is_file() {
            continue;
        }
        let path = dir.path().join("config.rs");
        let content = fs::read_to_string(&path)?;
        let parser = ServiceParser {
            service: dir.file_name().to_string_lossy().to_string(),
            path: path.to_string_lossy().to_string(),
            content,
        };
        let service = parser.parse().context(format!("path: {path:?}"))?;
        map.insert(parser.service, service);
    }

    Ok(Services(map))
}

/// ServiceParser is used to parse a service config file.
pub struct ServiceParser {
    service: String,
    path: String,
    content: String,
}

/// A typical service config will look like this:
///
/// ```
/// #[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
/// #[serde(default)]
/// #[non_exhaustive]
/// pub struct S3Config {
///     /// root of this backend.
///     ///
///     /// All operations will happen under this root.
///     ///
///     /// default to `/` if not set.
///     pub root: Option<String>,
///     /// bucket name of this backend.
///     ///
///     /// required.
///     pub bucket: String,
///     /// is bucket versioning enabled for this bucket
///     pub enable_versioning: bool,
/// }
/// ```
impl ServiceParser {
    /// Parse the content of this service.
    fn parse(&self) -> Result<Service> {
        debug!("service {} parse started", self.service);

        let ast = syn::parse_file(&self.content)?;

        let config_struct = ast
            .items
            .iter()
            .find_map(|v| {
                if let Item::Struct(v) = v {
                    if v.ident.to_string().contains("Config") {
                        return Some(v.clone());
                    }
                }
                None
            })
            .ok_or_else(|| anyhow!("there is no Config in {}", &self.path))?;

        let mut config = Vec::with_capacity(config_struct.fields.len());
        for field in config_struct.fields {
            let field = self.parse_field(field)?;
            config.push(field);
        }

        debug!("service {} parse finished", self.service);
        Ok(Service { config })
    }

    /// TODO: Add comment parse support.
    fn parse_field(&self, field: Field) -> Result<Config> {
        let name = field
            .ident
            .clone()
            .ok_or_else(|| anyhow!("field name is missing for {:?}", &field))?;

        let deprecated = self.deprecated_note(&field);

        let (cfg_type, optional) = match &field.ty {
            Type::Path(TypePath { path, .. }) => {
                let segment = path
                    .segments
                    .last()
                    .ok_or_else(|| anyhow!("config type must be provided for {field:?}"))?;

                let optional = segment.ident == "Option";

                let type_name = if optional {
                    if let PathArguments::AngleBracketed(args) = &segment.arguments {
                        if let Some(GenericArgument::Type(Type::Path(inner_path))) =
                            args.args.first()
                        {
                            if let Some(inner_segment) = inner_path.path.segments.last() {
                                inner_segment.ident.to_string()
                            } else {
                                unreachable!("Option must have segment")
                            }
                        } else {
                            unreachable!("Option must have GenericArgument")
                        }
                    } else {
                        unreachable!("Option must have angle bracketed arguments")
                    }
                } else {
                    segment.ident.to_string()
                };

                let typ = type_name.as_str().parse()?;
                let optional = optional || typ == ConfigType::Bool;

                (typ, optional)
            }
            v => return Err(anyhow!("unsupported config type {v:?}")),
        };

        deprecated = deprecated.map(|s| s[1..s.len() - 1].into());

        Ok(Config {
            name: name.to_string(),
            value: cfg_type,
            optional,
            deprecated,
            comments: "".to_string(),
        })
    }

    fn deprecated_note(&self, field: &Field) -> Option<String> {
        for attr in &field.attrs {
            if !attr.path().is_ident("deprecated") {
                continue;
            }

            let meta_list = match &attr.meta {
                syn::Meta::List(meta_list) => meta_list,
                _ => continue,
            };

            let tokens = Vec::from_iter(meta_list.tokens.clone().into_iter());
            for (index, token) in tokens.iter().enumerate() {
                let ident = match token {
                    proc_macro2::TokenTree::Ident(ident) => ident,
                    _ => {
                        continue;
                    }
                };

                if ident == "note" {
                    return tokens
                        .get(index + 2)
                        .expect("deprecated attribute missing note")
                        .span()
                        .source_text()
                        .map(|s| enquote::unquote(s.as_str()).expect("should unquote string"));
                }
            }
        }

        return None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;
    use std::path::PathBuf;
    use syn::ItemStruct;

    #[test]
    fn test_parse_field() {
        let cases = vec![
            (
                "pub root: Option<String>",
                Config {
                    name: "root".to_string(),
                    value: ConfigType::String,
                    optional: true,
                    deprecated: None,
                    comments: "".to_string(),
                },
            ),
            (
                "root: String",
                Config {
                    name: "root".to_string(),
                    value: ConfigType::String,
                    optional: false,
                    deprecated: None,
                    comments: "".to_string(),
                },
            ),
        ];

        for (input, expected) in cases {
            let input = format!("struct Test {{ {input} }}");
            let x: ItemStruct = syn::parse_str(&input).unwrap();
            let actual =
                ServiceParser::parse_field(x.fields.iter().next().unwrap().clone()).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_parse_service() {
        let content = r#"
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

/// Config for Aws S3 and compatible services (including minio, digitalocean space, Tencent Cloud Object Storage(COS) and so on) support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct S3Config {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// default to `/` if not set.
    pub root: Option<String>,
    /// bucket name of this backend.
    ///
    /// required.
    pub bucket: String,
    /// is bucket versioning enabled for this bucket
    pub enable_versioning: bool,
    /// endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`
    /// - Cloudflare R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`
    /// - Aliyun OSS: `https://{region}.aliyuncs.com`
    /// - Tencent COS: `https://cos.{region}.myqcloud.com`
    /// - Minio: `http://127.0.0.1:9000`
    ///
    /// If user inputs endpoint without scheme like "s3.amazonaws.com", we
    /// will prepend "https://" before it.
    ///
    /// - If endpoint is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - If still not set, default to `https://s3.amazonaws.com`.
    pub endpoint: Option<String>,
    /// Region represent the signing region of this endpoint. This is required
    /// if you are using the default AWS S3 endpoint.
    ///
    /// If using a custom endpoint,
    /// - If region is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub region: Option<String>,

    /// access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub access_key_id: Option<String>,
    /// secret_access_key of this backend.
    ///
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub secret_access_key: Option<String>,
    /// session_token (aka, security token) of this backend.
    ///
    /// This token will expire after sometime, it's recommended to set session_token
    /// by hand.
    pub session_token: Option<String>,
    /// role_arn for this backend.
    ///
    /// If `role_arn` is set, we will use already known config as source
    /// credential to assume role with `role_arn`.
    pub role_arn: Option<String>,
    /// external_id for this backend.
    pub external_id: Option<String>,
    /// role_session_name for this backend.
    pub role_session_name: Option<String>,
    /// Disable config load so that opendal will not load config from
    /// environment.
    ///
    /// For examples:
    ///
    /// - envs like `AWS_ACCESS_KEY_ID`
    /// - files like `~/.aws/config`
    pub disable_config_load: bool,
    /// Disable load credential from ec2 metadata.
    ///
    /// This option is used to disable the default behavior of opendal
    /// to load credential from ec2 metadata, a.k.a, IMDSv2
    pub disable_ec2_metadata: bool,
    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    pub allow_anonymous: bool,
    /// server_side_encryption for this backend.
    ///
    /// Available values: `AES256`, `aws:kms`.
    pub server_side_encryption: Option<String>,
    /// server_side_encryption_aws_kms_key_id for this backend
    ///
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is not set, S3 will use aws managed kms key to encrypt data.
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is a valid kms key id, S3 will use the provided kms key to encrypt data.
    /// - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be
    ///   returned.
    /// - If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id`
    ///   is a noop.
    pub server_side_encryption_aws_kms_key_id: Option<String>,
    /// server_side_encryption_customer_algorithm for this backend.
    ///
    /// Available values: `AES256`.
    pub server_side_encryption_customer_algorithm: Option<String>,
    /// server_side_encryption_customer_key for this backend.
    ///
    /// # Value
    ///
    /// base64 encoded key that matches algorithm specified in
    /// `server_side_encryption_customer_algorithm`.
    pub server_side_encryption_customer_key: Option<String>,
    /// Set server_side_encryption_customer_key_md5 for this backend.
    ///
    /// # Value
    ///
    /// MD5 digest of key specified in `server_side_encryption_customer_key`.
    pub server_side_encryption_customer_key_md5: Option<String>,
    /// default storage_class for this backend.
    ///
    /// Available values:
    /// - `DEEP_ARCHIVE`
    /// - `GLACIER`
    /// - `GLACIER_IR`
    /// - `INTELLIGENT_TIERING`
    /// - `ONEZONE_IA`
    /// - `OUTPOSTS`
    /// - `REDUCED_REDUNDANCY`
    /// - `STANDARD`
    /// - `STANDARD_IA`
    ///
    /// S3 compatible services don't support all of them
    pub default_storage_class: Option<String>,
    /// Enable virtual host style so that opendal will send API requests
    /// in virtual host style instead of path style.
    ///
    /// - By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`
    /// - Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
    pub enable_virtual_host_style: bool,
    /// Set maximum batch operations of this backend.
    ///
    /// Some compatible services have a limit on the number of operations in a batch request.
    /// For example, R2 could return `Internal Error` while batch delete 1000 files.
    ///
    /// Please tune this value based on services' document.
    #[deprecated(
        since = "0.52.0",
        note = "Please use `delete_max_size` instead of `batch_max_operations`"
    )]
    pub batch_max_operations: Option<usize>,
    /// Set the maximum delete size of this backend.
    ///
    /// Some compatible services have a limit on the number of operations in a batch request.
    /// For example, R2 could return `Internal Error` while batch delete 1000 files.
    ///
    /// Please tune this value based on services' document.
    pub delete_max_size: Option<usize>,
    /// Disable stat with override so that opendal will not send stat request with override queries.
    ///
    /// For example, R2 doesn't support stat with `response_content_type` query.
    pub disable_stat_with_override: bool,
    /// Checksum Algorithm to use when sending checksums in HTTP headers.
    /// This is necessary when writing to AWS S3 Buckets with Object Lock enabled for example.
    ///
    /// Available options:
    /// - "crc32c"
    pub checksum_algorithm: Option<String>,
    /// Disable write with if match so that opendal will not send write request with if match headers.
    ///
    /// For example, Ceph RADOS S3 doesn't support write with if match.
    pub disable_write_with_if_match: bool,
}

impl Debug for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("S3Config");

        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region);

        d.finish_non_exhaustive()
    }
}
"#;
        let parser = ServiceParser {
            service: "s3".to_string(),
            path: "test".to_string(),
            content: content.to_string(),
        };

        let service = parser.parse().unwrap();
        assert_eq!(service.config.len(), 26);
        assert_eq!(
            service.config[21],
            Config {
                name: "batch_max_operations".to_string(),
                value: ConfigType::Usize,
                optional: true,
                deprecated: Some(
                    "Please use `delete_max_size` instead of `batch_max_operations`".into()
                ),
                comments: "".to_string(),
            },
        );
        assert_eq!(
            service.config[25],
            Config {
                name: "disable_write_with_if_match".to_string(),
                value: ConfigType::Bool,
                optional: true,
                deprecated: None,
                comments: "".to_string(),
            },
        );
    }

    #[test]
    fn test_parse() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let path = manifest_dir
            .join("../core/src/services")
            .canonicalize()
            .unwrap();

        // Parse should just pass.
        let _ = parse(&path.to_string_lossy()).unwrap();
    }
}
