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

use async_trait::async_trait;
use http::StatusCode;
use log::debug;

use super::core::HuggingFaceCore;
use super::error::parse_error;
use super::lister::HuggingFaceLister;
use super::message::HuggingFaceStatus;
use crate::raw::*;
use crate::*;

/// [HuggingFace](https://huggingface.co/docs/huggingface_hub/package_reference/hf_api)'s API support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct HuggingFaceBuilder {
    repo_type: Option<String>,
    repo_id: Option<String>,
    revision: Option<String>,
    root: Option<String>,
    read_token: Option<String>,
}

impl Debug for HuggingFaceBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("repo_type", &self.repo_type);
        ds.field("repo_id", &self.repo_id);
        ds.field("revision", &self.revision);
        ds.field("root", &self.root);

        if self.read_token.is_some() {
            ds.field("read_token", &"<redacted>");
        }

        ds.finish()
    }
}

impl HuggingFaceBuilder {
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
            self.repo_type = Some(repo_type.to_string());
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
            self.repo_id = Some(repo_id.to_string());
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
            self.revision = Some(revision.to_string());
        }
        self
    }

    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string());
        }
        self
    }

    /// Set the read token of this backend.
    ///
    /// This is optional.
    pub fn read_token(&mut self, read_token: &str) -> &mut Self {
        if !read_token.is_empty() {
            self.read_token = Some(read_token.to_string());
        }
        self
    }
}

impl Builder for HuggingFaceBuilder {
    const SCHEME: Scheme = Scheme::HuggingFace;
    type Accessor = HuggingFaceBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = HuggingFaceBuilder::default();

        map.get("repo_type").map(|v| builder.repo_type(v));
        map.get("repo_id").map(|v| builder.repo_id(v));
        map.get("revision").map(|v| builder.revision(v));
        map.get("root").map(|v| builder.root(v));
        map.get("read_token").map(|v| builder.read_token(v));

        builder
    }

    /// Build a HuggingFaceBackend.
    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let repo_type = match &self.repo_type {
            Some(repo_type) => match repo_type.as_str() {
                "model" => Ok(RepoType::Model),
                "dataset" => Ok(RepoType::Dataset),
                "space" => Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "repo type \"space\" is unsupported",
                )),
                _ => Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    format!("unknown repo_type: {}", repo_type).as_str(),
                )
                .with_operation("Builder::build")
                .with_context("service", Scheme::HuggingFace)),
            },
            None => Ok(RepoType::Model),
        }?;
        debug!("backend use repo_type: {:?}", &repo_type);

        let repo_id = match &self.repo_id {
            Some(repo_id) => Ok(repo_id.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "repo_id is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::HuggingFace)),
        }?;
        debug!("backend use repo_id: {}", &repo_id);

        let revision = match &self.revision {
            Some(revision) => Ok(revision.clone()),
            None => Ok("main".to_string()),
        }?;
        debug!("backend use revision: {}", &revision);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root: {}", &root);

        let read_token = match &self.read_token {
            Some(read_token) => Some(read_token.clone()),
            None => None,
        };

        let client = HttpClient::new()?;

        debug!("backend build finished: {:?}", &self);
        Ok(HuggingFaceBackend {
            core: Arc::new(HuggingFaceCore {
                repo_type,
                repo_id,
                revision,
                root,
                read_token,
                client,
            }),
        })
    }
}

/// Backend for HuggingFace service
#[derive(Debug, Clone)]
pub struct HuggingFaceBackend {
    core: Arc<HuggingFaceCore>,
}

#[async_trait]
impl Accessor for HuggingFaceBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Lister = oio::PageLister<HuggingFaceLister>;
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::HuggingFace)
            .set_native_capability(Capability {
                stat: true,

                read: true,
                read_can_next: true,
                read_with_range: true,

                list: true,
                list_without_recursive: true,
                list_with_recursive: true,

                ..Default::default()
            });
        am
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.hf_read(path, args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let size = parse_content_length(resp.headers())?;
                Ok((RpRead::new().with_size(size), resp.into_body()))
            }
            StatusCode::RANGE_NOT_SATISFIABLE => Ok((RpRead::new(), IncomingAsyncBody::empty())),
            _ => Err(parse_error(resp).await?),
        }
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
                let bs = resp.into_body().bytes().await?;

                let decoded_response = serde_json::from_slice::<Vec<HuggingFaceStatus>>(&bs)
                    .map_err(new_json_deserialize_error)?;

                // NOTE: if the file is not found, the server will return 200 with an empty array
                if let Some(status) = decoded_response.get(0) {
                    if let Some(commit_info) = status.last_commit.as_ref() {
                        meta.set_last_modified(parse_datetime_from_rfc3339(
                            commit_info.date.as_str(),
                        )?);
                    }

                    match status.type_.as_str() {
                        "directory" => meta.set_mode(EntryMode::DIR),
                        "file" => meta.set_mode(EntryMode::FILE),
                        _ => return Err(Error::new(ErrorKind::Unexpected, "unknown status type")),
                    };
                } else {
                    if path.ends_with('/') {
                        meta.set_mode(EntryMode::DIR);
                    } else {
                        return Err(Error::new(ErrorKind::NotFound, "path not found"));
                    }
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = HuggingFaceLister::new(self.core.clone(), path.to_string(), args.recursive());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

/// Repository type of HuggingFace. Currently, we only support `model` and `dataset`.
/// [Reference](https://huggingface.co/docs/hub/repositories)
#[derive(Debug, Clone, Copy)]
pub enum RepoType {
    Model,
    Dataset,
}
