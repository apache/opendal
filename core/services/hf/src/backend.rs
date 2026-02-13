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

use std::sync::Arc;

use log::debug;

use super::HF_SCHEME;
use super::config::HfConfig;
use super::core::HfCore;
use super::deleter::HfDeleter;
use super::lister::HfLister;
use super::reader::HfReader;
use super::uri::{HfRepo, RepoType};
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
            if let Ok(rt) = RepoType::parse(repo_type) {
                self.config.repo_type = rt;
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

    /// Enable XET storage protocol for reads.
    ///
    /// When the `xet` feature is compiled in, reads will check for
    /// XET-backed files and use the XET protocol for downloading.
    /// Default is disabled.
    pub fn enable_xet(mut self) -> Self {
        self.config.xet = true;
        self
    }

    /// Disable XET storage protocol for reads.
    pub fn disable_xet(mut self) -> Self {
        self.config.xet = false;
        self
    }

    /// Set the maximum number of retries for commit operations.
    ///
    /// Retries on commit conflicts (HTTP 412) and transient server
    /// errors (HTTP 5xx). Default is 3.
    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.config.max_retries = Some(max_retries);
        self
    }
}

impl Builder for HfBuilder {
    type Config = HfConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let repo_type = self.config.repo_type;
        debug!("backend use repo_type: {:?}", &repo_type);

        let repo_id = match &self.config.repo_id {
            Some(repo_id) => Ok(repo_id.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "repo_id is empty")
                .with_operation("Builder::build")
                .with_context("service", HF_SCHEME)),
        }?;
        debug!("backend use repo_id: {}", &repo_id);

        let revision = match &self.config.revision {
            Some(revision) => revision.clone(),
            None => "main".to_string(),
        };
        debug!("backend use revision: {}", &revision);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root: {}", &root);

        let token = self.config.token.as_ref().cloned();

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                // Try to read from HF_ENDPOINT env var which is used
                // by the official huggingface clients.
                if let Ok(env_endpoint) = std::env::var("HF_ENDPOINT") {
                    env_endpoint
                } else {
                    "https://huggingface.co".to_string()
                }
            }
        };
        debug!("backend use endpoint: {}", &endpoint);

        let info: Arc<AccessorInfo> = {
            let am = AccessorInfo::default();
            am.set_scheme(HF_SCHEME)
                .set_native_capability(Capability {
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

        let max_retries = self.config.max_retries.unwrap_or(3);
        debug!("backend max_retries: {}", max_retries);

        Ok(HfBackend {
            core: Arc::new(HfCore::new(
                info,
                repo,
                root,
                token,
                endpoint,
                max_retries,
                #[cfg(feature = "xet")]
                self.config.xet,
            )?),
        })
    }
}

/// Backend for Hugging Face service
#[derive(Debug, Clone)]
pub struct HfBackend {
    pub(crate) core: Arc<HfCore>,
}

impl Access for HfBackend {
    type Reader = HfReader;
    type Writer = HfWriter;
    type Lister = oio::PageLister<HfLister>;
    type Deleter = oio::BatchDeleter<HfDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let info = self.core.path_info(path).await?;
        Ok(RpStat::new(info.metadata()?))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let reader = HfReader::try_new(&self.core, path, args.range()).await?;
        Ok((RpRead::default(), reader))
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
        let max_batch_size = self.core.info.full_capability().delete_max_size;
        Ok((
            RpDelete::default(),
            oio::BatchDeleter::new(deleter, max_batch_size),
        ))
    }
}

#[cfg(test)]
pub(super) mod test_utils {
    use super::HfBuilder;
    use opendal_core::Operator;
    use opendal_core::layers::HttpClientLayer;
    use opendal_core::raw::HttpClient;

    /// Create an operator with a fresh HTTP client so parallel tests
    /// don't share the global static reqwest client (which causes
    /// "dispatch task is gone" errors when runtimes are dropped).
    fn finish_operator(op: Operator) -> Operator {
        let client = HttpClient::with(reqwest::Client::new());
        op.layer(HttpClientLayer::new(client))
    }

    pub fn testing_credentials() -> (String, String) {
        let repo_id = std::env::var("HF_OPENDAL_DATASET").expect("HF_OPENDAL_DATASET must be set");
        let token = std::env::var("HF_OPENDAL_TOKEN").expect("HF_OPENDAL_TOKEN must be set");
        (repo_id, token)
    }

    #[cfg(feature = "xet")]
    pub fn testing_bucket_credentials() -> (String, String) {
        let repo_id = std::env::var("HF_OPENDAL_BUCKET").expect("HF_OPENDAL_BUCKET must be set");
        let token = std::env::var("HF_OPENDAL_TOKEN").expect("HF_OPENDAL_TOKEN must be set");
        (repo_id, token)
    }

    /// Operator for a private dataset requiring HF_OPENDAL_DATASET and HF_OPENDAL_TOKEN.
    /// Uses higher max_retries to tolerate concurrent commit conflicts (412).
    pub fn testing_operator() -> Operator {
        let (repo_id, token) = testing_credentials();
        let op = Operator::new(
            HfBuilder::default()
                .repo_type("dataset")
                .repo_id(&repo_id)
                .token(&token)
                .max_retries(10),
        )
        .unwrap()
        .finish();
        finish_operator(op)
    }

    #[cfg(feature = "xet")]
    pub fn testing_xet_operator() -> Operator {
        let (repo_id, token) = testing_credentials();
        let op = Operator::new(
            HfBuilder::default()
                .repo_type("dataset")
                .repo_id(&repo_id)
                .token(&token)
                .enable_xet()
                .max_retries(10),
        )
        .unwrap()
        .finish();
        finish_operator(op)
    }

    /// Operator for a bucket requiring HF_OPENDAL_BUCKET and HF_OPENDAL_TOKEN.
    /// Buckets always use XET for writes.
    #[cfg(feature = "xet")]
    pub fn testing_bucket_operator() -> Operator {
        let (repo_id, token) = testing_bucket_credentials();
        let op = Operator::new(
            HfBuilder::default()
                .repo_type("bucket")
                .repo_id(&repo_id)
                .token(&token)
                .enable_xet()
                .max_retries(10),
        )
        .unwrap()
        .finish();
        finish_operator(op)
    }

    pub fn gpt2_operator() -> Operator {
        let op = Operator::new(
            HfBuilder::default()
                .repo_type("model")
                .repo_id("openai-community/gpt2"),
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

    #[cfg(feature = "xet")]
    pub fn mbpp_xet_operator() -> Operator {
        let mut builder = HfBuilder::default()
            .repo_type("dataset")
            .repo_id("google-research-datasets/mbpp")
            .enable_xet();
        if let Ok(token) = std::env::var("HF_OPENDAL_TOKEN") {
            builder = builder.token(&token);
        }
        let op = Operator::new(builder).unwrap().finish();
        finish_operator(op)
    }
}

#[cfg(test)]
mod tests {
    use super::test_utils::mbpp_operator;
    #[cfg(feature = "xet")]
    use super::test_utils::mbpp_xet_operator;
    use super::*;

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

        let registry = OperatorRegistry::new();
        super::super::register_hf_service(&registry);

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

    /// Parquet magic bytes: "PAR1"
    const PARQUET_MAGIC: &[u8] = b"PAR1";

    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_parquet_http() {
        let op = mbpp_operator();
        let path = "full/train-00000-of-00001.parquet";

        let meta = op.stat(path).await.expect("stat should succeed");
        assert!(meta.content_length() > 0);

        // Read the first 4 bytes to check parquet header magic
        let header = op
            .read_with(path)
            .range(0..4)
            .await
            .expect("read header should succeed");
        assert_eq!(&header.to_vec(), PARQUET_MAGIC);

        // Read the last 4 bytes to check parquet footer magic
        let size = meta.content_length();
        let footer = op
            .read_with(path)
            .range(size - 4..size)
            .await
            .expect("read footer should succeed");
        assert_eq!(&footer.to_vec(), PARQUET_MAGIC);
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_parquet_xet() {
        let op = mbpp_xet_operator();
        let path = "full/train-00000-of-00001.parquet";

        // Full read via XET and verify parquet magic at both ends
        let data = op.read(path).await.expect("xet read should succeed");
        let bytes = data.to_vec();
        assert!(bytes.len() > 8);
        assert_eq!(&bytes[..4], PARQUET_MAGIC);
        assert_eq!(&bytes[bytes.len() - 4..], PARQUET_MAGIC);
    }

    /// List files in a known dataset directory.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_list_directory() {
        let op = mbpp_operator();
        let entries = op.list("full/").await.expect("list should succeed");
        assert!(!entries.is_empty(), "directory should contain files");
        assert!(
            entries.iter().any(|e| e.path().ends_with(".parquet")),
            "should contain parquet files"
        );
    }

    /// List files recursively from root.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_list_recursive() {
        let op = mbpp_operator();
        let entries = op
            .list_with("/")
            .recursive(true)
            .await
            .expect("recursive list should succeed");
        assert!(
            entries.len() > 1,
            "recursive listing should find multiple files"
        );
    }

    /// Stat a known file and verify metadata fields.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_stat_known_file() {
        let op = mbpp_operator();
        let meta = op
            .stat("full/train-00000-of-00001.parquet")
            .await
            .expect("stat should succeed");
        assert!(meta.content_length() > 0);
        assert!(!meta.etag().unwrap_or_default().is_empty());
    }

    /// Stat a nonexistent path should return NotFound.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_stat_nonexistent() {
        let op = mbpp_operator();
        let err = op
            .stat("this/path/does/not/exist.txt")
            .await
            .expect_err("stat on nonexistent path should fail");
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    /// Read a nonexistent file should return NotFound.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_nonexistent() {
        let op = mbpp_operator();
        let err = op
            .read("this/path/does/not/exist.txt")
            .await
            .expect_err("read on nonexistent path should fail");
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    /// Read a middle range of a known file.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_range_middle() {
        let op = mbpp_operator();
        let data = op
            .read_with("full/train-00000-of-00001.parquet")
            .range(100..200)
            .await
            .expect("range read should succeed");
        assert_eq!(data.to_bytes().len(), 100);
    }

    /// Read the last N bytes of a file to exercise tail-range handling.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_range_tail() {
        let op = mbpp_operator();
        let path = "full/train-00000-of-00001.parquet";
        let meta = op.stat(path).await.expect("stat should succeed");
        let size = meta.content_length();

        let data = op
            .read_with(path)
            .range(size - 100..size)
            .await
            .expect("tail range read should succeed");
        let bytes = data.to_bytes();
        assert_eq!(bytes.len(), 100);
        // Parquet files end with "PAR1" magic
        assert_eq!(&bytes[bytes.len() - 4..], PARQUET_MAGIC);
    }
}
