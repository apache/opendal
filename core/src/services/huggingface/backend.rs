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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use http::Response;
use http::StatusCode;
use log::debug;
use serde::{Deserialize, Serialize};

use super::core::HuggingfaceCore;
use super::core::HuggingfaceStatus;
use super::error::parse_error;
use super::lister::HuggingfaceLister;
use crate::raw::*;
use crate::*;

/// Configuration for Huggingface service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HuggingfaceConfig {
    /// Repo type of this backend. Default is model.
    ///
    /// Available values:
    /// - model
    /// - dataset
    pub repo_type: Option<String>,
    /// Repo id of this backend.
    ///
    /// This is required.
    pub repo_id: Option<String>,
    /// Revision of this backend.
    ///
    /// Default is main.
    pub revision: Option<String>,
    /// Root of this backend. Can be "/path/to/dir".
    ///
    /// Default is "/".
    pub root: Option<String>,
    /// Token of this backend.
    ///
    /// This is optional.
    pub token: Option<String>,
}

impl Debug for HuggingfaceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("HuggingfaceConfig");

        if let Some(repo_type) = &self.repo_type {
            ds.field("repo_type", &repo_type);
        }
        if let Some(repo_id) = &self.repo_id {
            ds.field("repo_id", &repo_id);
        }
        if let Some(revision) = &self.revision {
            ds.field("revision", &revision);
        }
        if let Some(root) = &self.root {
            ds.field("root", &root);
        }
        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

/// [Huggingface](https://huggingface.co/docs/huggingface_hub/package_reference/hf_api)'s API support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct HuggingfaceBuilder {
    config: HuggingfaceConfig,
}

impl Debug for HuggingfaceBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("config", &self.config);
        ds.finish()
    }
}

impl HuggingfaceBuilder {
    /// Set repo type of this backend. Default is model.
    ///
    /// Available values:
    /// - model
    /// - dataset
    ///
    /// Currently, only models and datasets are supported.
    /// [Reference](https://huggingface.co/docs/hub/repositories)
    pub fn repo_type(&mut self, repo_type: &str) -> &mut Self {
        if !repo_type.is_empty() {
            self.config.repo_type = Some(repo_type.to_string());
        }
        self
    }

    /// Set repo id of this backend. This is required.
    ///
    /// Repo id consists of the account name and the repository name.
    ///
    /// For example, model's repo id looks like:
    /// - meta-llama/Llama-2-7b
    ///
    /// Dataset's repo id looks like:
    /// - databricks/databricks-dolly-15k
    pub fn repo_id(&mut self, repo_id: &str) -> &mut Self {
        if !repo_id.is_empty() {
            self.config.repo_id = Some(repo_id.to_string());
        }
        self
    }

    /// Set revision of this backend. Default is main.
    ///
    /// Revision can be a branch name or a commit hash.
    ///
    /// For example, revision can be:
    /// - main
    /// - 1d0c4eb
    pub fn revision(&mut self, revision: &str) -> &mut Self {
        if !revision.is_empty() {
            self.config.revision = Some(revision.to_string());
        }
        self
    }

    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.config.root = Some(root.to_string());
        }
        self
    }

    /// Set the token of this backend.
    ///
    /// This is optional.
    pub fn token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }
}

impl Builder for HuggingfaceBuilder {
    const SCHEME: Scheme = Scheme::Huggingface;
    type Accessor = HuggingfaceBackend;
    type Config = HuggingfaceConfig;

    fn from_config(config: Self::Config) -> Self {
        Self { config }
    }

    fn from_map(map: HashMap<String, String>) -> Self {
        HuggingfaceConfig::deserialize(ConfigDeserializer::new(map))
            .map(Self::from_config)
            .expect("config deserialize must succeed")
    }

    /// Build a HuggingfaceBackend.
    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let repo_type = match self.config.repo_type.as_deref() {
            Some("model") => Ok(RepoType::Model),
            Some("dataset") => Ok(RepoType::Dataset),
            Some("space") => Err(Error::new(
                ErrorKind::ConfigInvalid,
                "repo type \"space\" is unsupported",
            )),
            Some(repo_type) => Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("unknown repo_type: {}", repo_type).as_str(),
            )
            .with_operation("Builder::build")
            .with_context("service", Scheme::Huggingface)),
            None => Ok(RepoType::Model),
        }?;
        debug!("backend use repo_type: {:?}", &repo_type);

        let repo_id = match &self.config.repo_id {
            Some(repo_id) => Ok(repo_id.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "repo_id is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Huggingface)),
        }?;
        debug!("backend use repo_id: {}", &repo_id);

        let revision = match &self.config.revision {
            Some(revision) => revision.clone(),
            None => "main".to_string(),
        };
        debug!("backend use revision: {}", &revision);

        let root = normalize_root(&self.config.root.take().unwrap_or_default());
        debug!("backend use root: {}", &root);

        let token = self.config.token.as_ref().cloned();

        let client = HttpClient::new()?;

        debug!("backend build finished: {:?}", &self);
        Ok(HuggingfaceBackend {
            core: Arc::new(HuggingfaceCore {
                repo_type,
                repo_id,
                revision,
                root,
                token,
                client,
            }),
        })
    }
}

/// Backend for Huggingface service
#[derive(Debug, Clone)]
pub struct HuggingfaceBackend {
    core: Arc<HuggingfaceCore>,
}

impl Access for HuggingfaceBackend {
    type Reader = HttpBody;
    type Writer = ();
    type Lister = oio::PageLister<HuggingfaceLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Huggingface)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                list: true,
                list_with_recursive: true,

                ..Default::default()
            });
        am.into()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.core.hf_path_info(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let mut meta = parse_into_metadata(path, resp.headers())?;
                let bs = resp.into_body();

                let decoded_response: Vec<HuggingfaceStatus> =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                // NOTE: if the file is not found, the server will return 200 with an empty array
                if let Some(status) = decoded_response.first() {
                    if let Some(commit_info) = status.last_commit.as_ref() {
                        meta.set_last_modified(parse_datetime_from_rfc3339(
                            commit_info.date.as_str(),
                        )?);
                    }

                    meta.set_content_length(status.size);

                    match status.type_.as_str() {
                        "directory" => meta.set_mode(EntryMode::DIR),
                        "file" => meta.set_mode(EntryMode::FILE),
                        _ => return Err(Error::new(ErrorKind::Unexpected, "unknown status type")),
                    };
                } else {
                    return Err(Error::new(ErrorKind::NotFound, "path not found"));
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.hf_resolve(path, args.range(), &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)).await?)
            }
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = HuggingfaceLister::new(self.core.clone(), path.to_string(), args.recursive());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

/// Repository type of Huggingface. Currently, we only support `model` and `dataset`.
/// [Reference](https://huggingface.co/docs/hub/repositories)
#[derive(Debug, Clone, Copy)]
pub enum RepoType {
    Model,
    Dataset,
}
