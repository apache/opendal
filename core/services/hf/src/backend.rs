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

use std::path::PathBuf;
use std::sync::Arc;

use log::debug;

use super::HF_SCHEME;
use super::config::HfConfig;
use super::core::HfCore;
use super::core::HfDownloadMode;
use super::deleter::HfDeleter;
use super::lister::HfLister;
use super::reader::HfReadStream;
use super::uri::{HfRepo, HfRepoType};
use super::writer::HfWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// [Hugging Face](https://huggingface.co/docs/huggingface_hub/package_reference/hf_api)'s API support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct HfBuilder {
    pub(super) config: HfConfig,
}

impl HfBuilder {
    /// Set repo type of this backend. Default is model.
    ///
    /// Available values:
    /// - model
    /// - dataset
    /// - datasets (alias for dataset)
    /// - space
    /// - bucket
    ///
    /// [Reference](https://huggingface.co/docs/hub/repositories)
    pub fn repo_type(mut self, repo_type: &str) -> Self {
        if !repo_type.is_empty() {
            if let Ok(rt) = HfRepoType::parse(repo_type) {
                self.config.repo_type = Some(rt);
            }
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
    pub fn repo_id(mut self, repo_id: &str) -> Self {
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
    pub fn revision(mut self, revision: &str) -> Self {
        if !revision.is_empty() {
            self.config.revision = Some(revision.to_string());
        }
        self
    }

    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set the token of this backend.
    ///
    /// This is optional.
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }

    /// Set the download mode. Either `xet` (default) or `http`.
    ///
    /// - `xet`: uses the XET protocol for downloads (default).
    /// - `http`: plain HTTP download, following the redirect from the server.
    pub fn download_mode(mut self, mode: &str) -> Self {
        if !mode.is_empty() {
            if let Ok(m) = HfDownloadMode::parse(mode) {
                self.config.download_mode = Some(m);
            }
        }
        self
    }

    /// configure the Hub base url. You might want to set this variable if your
    /// organization is using a Private Hub https://huggingface.co/enterprise
    ///
    /// Default is "https://huggingface.co"
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.to_string());
        }
        self
    }

    fn hf_endpoint(&self) -> String {
        self.config
            .endpoint
            .clone()
            .or_else(|| std::env::var("HF_ENDPOINT").ok())
            .unwrap_or_else(|| "https://huggingface.co".to_string())
    }

    fn hf_home() -> Option<PathBuf> {
        if let Ok(h) = std::env::var("HF_HOME") {
            return Some(PathBuf::from(h));
        }
        if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
            return Some(PathBuf::from(xdg).join("huggingface"));
        }
        let home = std::env::var("HOME").ok()?;
        Some(PathBuf::from(home).join(".cache/huggingface"))
    }

    /// Resolve the authentication token using the same priority order as hf-hub:
    /// explicit config → HF_HUB_DISABLE_IMPLICIT_TOKEN check → HF_TOKEN env → token file.
    fn hf_token(&self) -> Option<String> {
        if let Some(t) = self.config.token.clone() {
            return Some(t);
        }
        if let Ok(val) = std::env::var("HF_HUB_DISABLE_IMPLICIT_TOKEN") {
            if !val.is_empty() {
                return None;
            }
        }
        if let Ok(t) = std::env::var("HF_TOKEN") {
            if !t.is_empty() {
                return Some(t);
            }
        }
        let token_path = if let Ok(p) = std::env::var("HF_TOKEN_PATH") {
            Some(PathBuf::from(p))
        } else {
            Self::hf_home().map(|h| h.join("token"))
        };
        token_path
            .and_then(|p| std::fs::read_to_string(p).ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }
}

impl Builder for HfBuilder {
    type Config = HfConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let token = self.hf_token();
        let endpoint = self.hf_endpoint();

        let repo_type = self.config.repo_type.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "repo_type is required")
                .with_operation("Builder::build")
                .with_context("service", HF_SCHEME)
        })?;
        debug!("backend use repo_type: {:?}", &repo_type);

        let repo_id = self.config.repo_id.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "repo_id is required")
                .with_operation("Builder::build")
                .with_context("service", HF_SCHEME)
        })?;
        debug!("backend use repo_id: {}", &repo_id);

        let revision = match &self.config.revision {
            Some(revision) => revision.clone(),
            None => "main".to_string(),
        };
        debug!("backend use revision: {}", &revision);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root: {}", &root);
        debug!("backend use token: {}", token.is_some());
        debug!("backend use endpoint: {}", &endpoint);
        let download_mode = self.config.download_mode.unwrap_or_default();
        debug!("backend use download_mode: {:?}", download_mode);

        let info: Arc<AccessorInfo> = {
            let am = AccessorInfo::default();
            am.set_scheme(HF_SCHEME).set_service_capability(Capability {
                stat: true,
                read: true,
                write: token.is_some(),
                delete: token.is_some(),
                delete_max_size: Some(100),
                list: true,
                list_with_recursive: true,
                shared: true,
                ..Default::default()
            });
            am.into()
        };

        let repo = HfRepo::new(repo_type, repo_id, Some(revision.clone()));
        debug!("backend repo uri: {:?}", repo.uri(&root, ""));

        Ok(HfBackend {
            core: Arc::new(HfCore::build(
                info,
                repo,
                root,
                token,
                endpoint,
                download_mode,
            )?),
        })
    }
}

/// Backend for Hugging Face service
#[derive(Debug, Clone)]
pub struct HfBackend {
    pub(crate) core: Arc<HfCore>,
}

/// Reader returned by this backend.
pub struct HfReader {
    backend: HfBackend,
    path: String,
}

impl HfReader {
    fn new(backend: HfBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for HfReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let (rp, stream) = HfReadStream::try_new(&backend.core, path, range).await?;
        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

impl Access for HfBackend {
    type Reader = oio::StreamReader<HfReader>;
    type Writer = HfWriter;
    type Lister = oio::PageLister<HfLister>;
    type Deleter = oio::BatchDeleter<HfDeleter>;
    type Copier = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        // Buckets have no git directory entries; treat any trailing-slash path as a virtual dir.
        if self.core.repo.is_bucket() && path.ends_with('/') {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let info = self.core.path_info(path).await?;
        Ok(RpStat::new(info.metadata()?))
    }
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            oio::StreamReader::new(HfReader::new(self.clone(), path, args)),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let lister = HfLister::new(self.core.clone(), path.to_string(), args.recursive());
        Ok((RpList::default(), oio::PageLister::new(lister)))
    }

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = HfWriter::try_new(self.core.clone(), path.to_string()).await?;
        Ok((RpWrite::default(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter = HfDeleter::new(self.core.clone());
        let max_batch_size = self.core.info.capability().delete_max_size;
        Ok((
            RpDelete::default(),
            oio::BatchDeleter::new(deleter, max_batch_size),
        ))
    }
}

#[cfg(test)]
pub(super) mod test_utils {
    use std::sync::Arc;

    use super::super::core::{HfCore, HfDownloadMode};
    use super::super::uri::{HfRepo, HfRepoType};
    use super::HfBuilder;
    use opendal_core::Capability;
    use opendal_core::Operator;
    use opendal_core::layers::HttpClientLayer;
    use opendal_core::raw::{AccessorInfo, HttpClient};

    fn finish_operator(op: Operator) -> Operator {
        let client = HttpClient::with(reqwest::Client::new());
        op.layer(HttpClientLayer::new(client))
    }

    pub fn gpt2_operator() -> Operator {
        let op = Operator::new(
            HfBuilder::default()
                .repo_type("model")
                .repo_id("openai-community/gpt2")
                .download_mode("http"),
        )
        .unwrap()
        .finish();
        finish_operator(op)
    }

    pub fn mbpp_operator() -> Operator {
        let op = Operator::new(
            HfBuilder::default()
                .repo_type("dataset")
                .repo_id("google-research-datasets/mbpp"),
        )
        .unwrap()
        .finish();
        finish_operator(op)
    }

    pub fn testing_dataset_core() -> Arc<HfCore> {
        let repo_id = std::env::var("HF_OPENDAL_DATASET").expect("HF_OPENDAL_DATASET must be set");
        let token = std::env::var("HF_OPENDAL_TOKEN").expect("HF_OPENDAL_TOKEN must be set");

        let info = AccessorInfo::default();
        info.set_scheme("hf").set_service_capability(Capability {
            read: true,
            write: true,
            delete: true,
            ..Default::default()
        });
        info.update_http_client(|_| HttpClient::with(reqwest::Client::new()));

        let repo = HfRepo::new(HfRepoType::Dataset, repo_id, Some("main".to_string()));

        Arc::new(
            HfCore::build(
                Arc::new(info),
                repo,
                "/".to_string(),
                Some(token),
                "https://huggingface.co".to_string(),
                HfDownloadMode::Xet,
            )
            .expect("failed to build HfCore"),
        )
    }

    pub fn testing_bucket_operator() -> Operator {
        let repo_id = std::env::var("HF_OPENDAL_BUCKET").expect("HF_OPENDAL_BUCKET must be set");
        let token = std::env::var("HF_OPENDAL_TOKEN").expect("HF_OPENDAL_TOKEN must be set");
        let op = Operator::new(
            HfBuilder::default()
                .repo_type("bucket")
                .repo_id(&repo_id)
                .token(&token),
        )
        .unwrap()
        .finish();
        finish_operator(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env vars are process-global; serialize all tests that mutate them.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn builder_with_token(token: &str) -> HfBuilder {
        HfBuilder::default().token(token)
    }

    fn builder_no_token() -> HfBuilder {
        HfBuilder::default()
    }

    #[test]
    fn hf_token_config_takes_priority_over_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("HF_TOKEN", "env-token") };
        let result = builder_with_token("config-token").hf_token();
        unsafe { std::env::remove_var("HF_TOKEN") };
        assert_eq!(result.as_deref(), Some("config-token"));
    }

    #[test]
    fn hf_token_reads_hf_token_env_var() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::remove_var("HF_HUB_DISABLE_IMPLICIT_TOKEN") };
        unsafe { std::env::remove_var("HF_TOKEN_PATH") };
        unsafe { std::env::set_var("HF_TOKEN", "my-env-token") };
        let result = builder_no_token().hf_token();
        unsafe { std::env::remove_var("HF_TOKEN") };
        assert_eq!(result.as_deref(), Some("my-env-token"));
    }

    #[test]
    fn hf_token_disable_flag_suppresses_discovery() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1") };
        unsafe { std::env::set_var("HF_TOKEN", "my-env-token") };
        let result = builder_no_token().hf_token();
        unsafe { std::env::remove_var("HF_HUB_DISABLE_IMPLICIT_TOKEN") };
        unsafe { std::env::remove_var("HF_TOKEN") };
        assert_eq!(result, None);
    }

    #[test]
    fn hf_token_reads_from_file_via_hf_token_path() {
        let _guard = ENV_LOCK.lock().unwrap();
        let token_file = std::env::temp_dir().join("opendal-hf-token-test");
        std::fs::write(&token_file, "file-token\n").unwrap();
        unsafe { std::env::remove_var("HF_HUB_DISABLE_IMPLICIT_TOKEN") };
        unsafe { std::env::remove_var("HF_TOKEN") };
        unsafe { std::env::set_var("HF_TOKEN_PATH", &token_file) };
        let result = builder_no_token().hf_token();
        unsafe { std::env::remove_var("HF_TOKEN_PATH") };
        std::fs::remove_file(&token_file).ok();
        assert_eq!(result.as_deref(), Some("file-token"));
    }

    #[test]
    fn hf_endpoint_returns_default() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::remove_var("HF_ENDPOINT") };
        let result = HfBuilder::default().hf_endpoint();
        assert_eq!(result, "https://huggingface.co");
    }

    #[test]
    fn hf_endpoint_config_takes_priority_over_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("HF_ENDPOINT", "https://env.example.com") };
        let result = HfBuilder::default()
            .endpoint("https://config.example.com")
            .hf_endpoint();
        unsafe { std::env::remove_var("HF_ENDPOINT") };
        assert_eq!(result, "https://config.example.com");
    }

    #[test]
    fn hf_endpoint_reads_hf_endpoint_env_var() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("HF_ENDPOINT", "https://env.example.com") };
        let result = HfBuilder::default().hf_endpoint();
        unsafe { std::env::remove_var("HF_ENDPOINT") };
        assert_eq!(result, "https://env.example.com");
    }

    #[test]
    fn build_accepts_datasets_alias() {
        HfBuilder::default()
            .repo_id("org/repo")
            .repo_type("datasets")
            .build()
            .expect("builder should accept datasets alias");
    }

    #[test]
    fn build_accepts_space_repo_type() {
        HfBuilder::default()
            .repo_id("org/space")
            .repo_type("space")
            .build()
            .expect("builder should accept space repo type");
    }

    #[test]
    fn test_both_schemes_are_supported() {
        use opendal_core::OperatorRegistry;

        let registry = OperatorRegistry::get();
        super::super::register_hf_service(registry);

        // Test short scheme "hf"
        let op = registry
            .load("hf://user/repo")
            .expect("short scheme should be registered and work");
        assert_eq!(op.info().scheme(), "hf");

        // Test long scheme "huggingface"
        let op = registry
            .load("huggingface://user/repo")
            .expect("long scheme should be registered and work");
        assert_eq!(op.info().scheme(), "hf");
    }
}
