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
use std::fmt::Write;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BufMut;
use bytes::BytesMut;
use http::header::HeaderName;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign::AzureStorageSigner;
use uuid::Uuid;

use super::error::parse_error;
use super::error::parse_http_error;
use super::pager::AzblobPager;
use super::writer::AzblobWriter;
use crate::ops::*;
use crate::raw::*;
use crate::types::Metadata;
use crate::*;

const X_MS_BLOB_TYPE: &str = "x-ms-blob-type";

/// Known endpoint suffix Azure Storage Blob services resource URI syntax.
/// Azure public cloud: https://accountname.blob.core.windows.net
/// Azure US Government: https://accountname.blob.core.usgovcloudapi.net
/// Azure China: https://accountname.blob.core.chinacloudapi.cn
const KNOWN_AZBLOB_ENDPOINT_SUFFIX: &[&str] = &[
    "blob.core.windows.net",
    "blob.core.usgovcloudapi.net",
    "blob.core.chinacloudapi.cn",
];

const AZBLOB_BATCH_LIMIT: usize = 256;

/// Azure Storage Blob services support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [x] scan
/// - [ ] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work dir for backend.
/// - `container`: Set the container name for backend.
/// - `endpoint`: Set the endpoint for backend.
/// - `account_name`: Set the account_name for backend.
/// - `account_key`: Set the account_key for backend.
///
/// Refer to public API docs for more information.
///
/// # Example
///
/// This example works on [Azurite](https://github.com/Azure/Azurite) for local developments.
///
/// ## Start local blob service
///
/// ```shell
/// docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
/// az storage container create --name test --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
/// ```
///
/// ## Init OpenDAL Operator
///
/// ### Via Builder
///
/// ```no_run
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::services::Azblob;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create azblob backend builder.
///     let mut builder = Azblob::default();
///     // Set the root for azblob, all operations will happen under this root.
///     //
///     // NOTE: the root must be absolute path.
///     builder.root("/path/to/dir");
///     // Set the container name, this is required.
///     builder.container("test");
///     // Set the endpoint, this is required.
///     //
///     // For examples:
///     // - "http://127.0.0.1:10000/devstoreaccount1"
///     // - "https://accountname.blob.core.windows.net"
///     builder.endpoint("http://127.0.0.1:10000/devstoreaccount1");
///     // Set the account_name and account_key.
///     //
///     // OpenDAL will try load credential from the env.
///     // If credential not set and no valid credential in env, OpenDAL will
///     // send request without signing like anonymous user.
///     builder.account_name("devstoreaccount1");
///     builder.account_key("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
///
///     // `Accessor` provides the low level APIs, we will use `Operator` normally.
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
#[derive(Default, Clone)]
pub struct AzblobBuilder {
    root: Option<String>,
    container: String,
    endpoint: Option<String>,
    account_name: Option<String>,
    account_key: Option<String>,
    sas_token: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for AzblobBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("root", &self.root);
        ds.field("container", &self.container);
        ds.field("endpoint", &self.endpoint);

        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }
        if self.sas_token.is_some() {
            ds.field("sas_token", &"<redacted>");
        }

        ds.finish()
    }
}

impl AzblobBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

        self
    }

    /// Set container name of this backend.
    pub fn container(&mut self, container: &str) -> &mut Self {
        self.container = container.to_string();

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azblob: `https://accountname.blob.core.windows.net`
    /// - Azurite: `http://127.0.0.1:10000/devstoreaccount1`
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Set account_name of this backend.
    ///
    /// - If account_name is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn account_name(&mut self, account_name: &str) -> &mut Self {
        if !account_name.is_empty() {
            self.account_name = Some(account_name.to_string());
        }

        self
    }

    /// Set account_key of this backend.
    ///
    /// - If account_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn account_key(&mut self, account_key: &str) -> &mut Self {
        if !account_key.is_empty() {
            self.account_key = Some(account_key.to_string());
        }

        self
    }

    /// Set sas_token of this backend.
    ///
    /// - If sas_token is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    ///
    /// See [Grant limited access to Azure Storage resources using shared access signatures (SAS)](https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview)
    /// for more info.
    pub fn sas_token(&mut self, sas_token: &str) -> &mut Self {
        if !sas_token.is_empty() {
            self.sas_token = Some(sas_token.to_string());
        }

        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(&mut self, client: HttpClient) -> &mut Self {
        self.http_client = Some(client);
        self
    }

    /// from_connection_string will make a builder from connection string
    ///
    /// connection string looks like:
    ///
    /// ```txt
    /// DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;
    /// AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
    /// BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;
    /// QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;
    /// TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
    /// ```
    ///
    /// Or
    ///
    /// ```txt
    /// DefaultEndpointsProtocol=https;
    /// AccountName=storagesample;
    /// AccountKey=<account-key>;
    /// EndpointSuffix=core.chinacloudapi.cn;
    /// ```
    ///
    /// For reference: [Configure Azure Storage connection strings](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string)
    ///
    /// # Note
    ///
    /// connection string only configures the endpoint, account name and account key.
    /// User still needs to configure bucket names.
    pub fn from_connection_string(conn: &str) -> Result<Self> {
        let conn = conn.trim().replace('\n', "");

        let mut conn_map: HashMap<_, _> = HashMap::default();
        for v in conn.split(';') {
            let entry: Vec<_> = v.splitn(2, '=').collect();
            if entry.len() != 2 {
                // Ignore invalid entries.
                continue;
            }
            conn_map.insert(entry[0], entry[1]);
        }

        let mut builder = AzblobBuilder::default();

        if let Some(sas_token) = conn_map.get("SharedAccessSignature") {
            builder.sas_token(sas_token);
        } else {
            let account_name = conn_map.get("AccountName").ok_or_else(|| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "connection string must have AccountName",
                )
                .with_operation("Builder::from_connection_string")
            })?;
            builder.account_name(account_name);
            let account_key = conn_map.get("AccountKey").ok_or_else(|| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "connection string must have AccountKey",
                )
                .with_operation("Builder::from_connection_string")
            })?;
            builder.account_key(account_key);
        }

        if let Some(v) = conn_map.get("BlobEndpoint") {
            builder.endpoint(v);
        } else if let Some(v) = conn_map.get("EndpointSuffix") {
            let protocol = conn_map.get("DefaultEndpointsProtocol").unwrap_or(&"https");
            let account_name = builder
                .account_name
                .as_ref()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "connection string must have AccountName",
                    )
                    .with_operation("Builder::from_connection_string")
                })?
                .clone();
            builder.endpoint(&format!("{protocol}://{account_name}.blob.{v}"));
        }

        Ok(builder)
    }
}

impl Builder for AzblobBuilder {
    const SCHEME: Scheme = Scheme::Azblob;
    type Accessor = AzblobBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = AzblobBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("container").map(|v| builder.container(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("account_name").map(|v| builder.account_name(v));
        map.get("account_key").map(|v| builder.account_key(v));
        map.get("sas_token").map(|v| builder.sas_token(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle endpoint, region and container name.
        let container = match self.container.is_empty() {
            false => Ok(&self.container),
            true => Err(Error::new(ErrorKind::ConfigInvalid, "container is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azblob)),
        }?;
        debug!("backend use container {}", &container);

        let endpoint = match &self.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azblob)),
        }?;
        debug!("backend use endpoint {}", &container);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Azblob)
            })?
        };

        let mut signer_builder = AzureStorageSigner::builder();
        let mut account_name: Option<String> = None;
        if let Some(sas_token) = &self.sas_token {
            signer_builder.security_token(sas_token);
            match &self.account_name {
                Some(name) => account_name = Some(name.clone()),
                None => {
                    account_name = infer_storage_name_from_endpoint(endpoint.as_str());
                }
            }
        } else if let (Some(name), Some(key)) = (&self.account_name, &self.account_key) {
            account_name = Some(name.clone());
            signer_builder.account_name(name).account_key(key);
        } else if let Some(key) = &self.account_key {
            account_name = infer_storage_name_from_endpoint(endpoint.as_str());
            signer_builder
                .account_name(account_name.as_ref().unwrap_or(&String::new()))
                .account_key(key);
        }

        let signer = signer_builder.clone().build().map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "build AzureStorageSigner")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azblob)
                .with_context("endpoint", &endpoint)
                .with_context("container", container.as_str())
                .set_source(e)
        })?;
        signer_builder.omit_service_version();
        let sub_req_signer = signer_builder.build().map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "build AzureStorageSigner")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Azblob)
                .with_context("endpoint", &endpoint)
                .with_context("container", container.as_str())
                .set_source(e)
        })?;

        debug!("backend build finished: {:?}", &self);
        Ok(AzblobBackend {
            root,
            endpoint,
            signer: Arc::new(signer),
            batch_signer: Arc::new(sub_req_signer),
            container: self.container.clone(),
            client,
            _account_name: account_name.unwrap_or_default(),
        })
    }
}

fn infer_storage_name_from_endpoint(endpoint: &str) -> Option<String> {
    let _endpoint: &str = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);

    let mut parts = _endpoint.splitn(2, '.');
    let storage_name = parts.next();
    let endpoint_suffix = parts
        .next()
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_lowercase();

    if KNOWN_AZBLOB_ENDPOINT_SUFFIX
        .iter()
        .any(|s| *s == endpoint_suffix.as_str())
    {
        storage_name.map(|s| s.to_string())
    } else {
        None
    }
}

/// Backend for azblob services.
#[derive(Debug, Clone)]
pub struct AzblobBackend {
    container: String,
    // TODO: remove pub after https://github.com/datafuselabs/opendal/issues/1427
    pub client: HttpClient,
    root: String, // root will be "/" or /abc/
    endpoint: String,
    pub signer: Arc<AzureStorageSigner>,
    pub batch_signer: Arc<AzureStorageSigner>,
    _account_name: String,
}

#[async_trait]
impl Accessor for AzblobBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = AzblobWriter;
    type BlockingWriter = ();
    type Pager = AzblobPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        use AccessorCapability::*;
        use AccessorHint::*;

        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Azblob)
            .set_root(&self.root)
            .set_name(&self.container)
            .set_capabilities(Read | Write | List | Scan | Batch)
            .set_hints(ReadStreamable);

        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let mut req = self.azblob_put_blob_request(path, Some(0), None, AsyncBody::Empty)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        let resp = self.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCreate::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.azblob_get_blob(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;

                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.append() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "append write is not supported",
            ));
        }

        Ok((
            RpWrite::default(),
            AzblobWriter::new(self.clone(), args, path.to_string()),
        ))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.azblob_get_blob_properties(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.azblob_delete_blob(path).await?;

        let status = resp.status();

        match status {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let op = AzblobPager::new(
            Arc::new(self.clone()),
            self.root.clone(),
            path.to_string(),
            "/".to_string(),
            args.limit(),
        );

        Ok((RpList::default(), op))
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        let op = AzblobPager::new(
            Arc::new(self.clone()),
            self.root.clone(),
            path.to_string(),
            "".to_string(),
            args.limit(),
        );

        Ok((RpScan::default(), op))
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let ops = args.into_operation();
        match ops {
            BatchOperations::Delete(ops) => {
                let paths = ops.into_iter().map(|(p, _)| p).collect::<Vec<_>>();
                // vector to store all the results
                let mut results = Vec::with_capacity(paths.len());

                let batches = paths.chunks(AZBLOB_BATCH_LIMIT);
                for batch in batches {
                    let batch = batch.to_vec();
                    // construct and complete batch request
                    let resp = self.azblob_batch_del(batch.iter()).await?;

                    // check response status
                    if resp.status() != StatusCode::ACCEPTED {
                        return Err(parse_error(resp).await?);
                    }

                    // get boundary from response header
                    let content_type = resp.headers().get(CONTENT_TYPE).ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "response data should have CONTENT_TYPE header",
                        )
                    })?;
                    let content_type =
                        content_type
                            .to_str()
                            .map(|ty| ty.to_string())
                            .map_err(|e| {
                                Error::new(
                                    ErrorKind::Unexpected,
                                    &format!(
                                        "get invalid CONTENT_TYPE header in response: {:?}",
                                        e
                                    ),
                                )
                            })?;
                    let splits = content_type.split("boundary=").collect::<Vec<&str>>();
                    let boundary = splits.get(1).to_owned().ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "No boundary message provided in CONTENT_TYPE",
                        )
                    })?;

                    let body = resp.into_body().bytes().await?;
                    let body = String::from_utf8(body.to_vec()).map_err(|e| {
                        Error::new(
                            ErrorKind::Unexpected,
                            &format!("get invalid batch response {e:?}"),
                        )
                    })?;

                    let batch_results = batch_del_resp_parse(boundary, body, batch)?;
                    results.extend(batch_results);
                }
                Ok(RpBatch::new(BatchedResults::Delete(results)))
            }
        }
    }
}

impl AzblobBackend {
    async fn azblob_get_blob(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            // azblob doesn't support read with suffix range.
            //
            // ref: https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-the-range-header-for-blob-service-operations
            if range.offset().is_none() && range.size().is_some() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "azblob doesn't support read with suffix range",
                ));
            }

            req = req.header(http::header::RANGE, range.to_header());
        }

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub fn azblob_put_blob_request(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        if let Some(ty) = content_type {
            req = req.header(CONTENT_TYPE, ty)
        }

        req = req.header(HeaderName::from_static(X_MS_BLOB_TYPE), "BlockBlob");

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    async fn azblob_get_blob_properties(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    async fn azblob_delete_blob(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub(crate) async fn azblob_list_blobs(
        &self,
        path: &str,
        next_marker: &str,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/{}?restype=container&comp=list",
            self.endpoint, self.container
        );
        if !p.is_empty() {
            write!(url, "&prefix={}", percent_encode_path(&p))
                .expect("write into string must succeed");
        }
        if let Some(limit) = limit {
            write!(url, "&maxresults={limit}").expect("write into string must succeed");
        }
        if !delimiter.is_empty() {
            write!(url, "&delimiter={delimiter}").expect("write into string must succeed");
        }
        if !next_marker.is_empty() {
            write!(url, "&marker={next_marker}").expect("write into string must succeed");
        }

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    async fn azblob_batch_del(
        &self,
        paths: impl Iterator<Item = &String>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let boundary = format!("opendal-{}", Uuid::new_v4());
        // init batch request
        let url = format!(
            "{}/{}?restype=container&comp=batch",
            self.endpoint, self.container
        );
        let req_builder = Request::post(&url).header(
            CONTENT_TYPE,
            format!("multipart/mixed; boundary={}", boundary),
        );

        let mut body = BytesMut::new();

        for (idx, path) in paths.into_iter().enumerate() {
            // build sub requests
            let p = build_abs_path(&self.root, path);
            let encoded_path = percent_encode_path(&p);

            let url = Uri::from_str(&format!(
                "{}/{}/{}",
                self.endpoint, self.container, encoded_path
            ))
            .unwrap();
            let path = url.path();

            let block = format!(
                r#"--{boundary}
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: {idx}

DELETE {path} HTTP/1.1
"#
            );
            // replace LF with CRLF, required by Azure Storage Blobs service.
            //
            // The Rust compiler converts all CRLF sequences to LF when reading source files
            // since 2019, so it is safe to convert here
            let mut block = block.replace('\n', "\r\n");

            let mut sub_req = Request::delete(&url.to_string())
                .header(CONTENT_LENGTH, 0)
                .body(AsyncBody::Empty)
                .map_err(new_request_build_error)?;
            self.batch_signer
                .sign(&mut sub_req)
                .map_err(new_request_sign_error)?;

            let headers: Vec<(String, String)> = sub_req
                .headers()
                .iter()
                .filter_map(|(k, v)| {
                    let (k, v) = (
                        k.as_str().to_string(),
                        v.to_str().expect("must be valid header").to_string(),
                    );
                    if k.to_lowercase() == "x-ms-version" {
                        None
                    } else {
                        Some((k, v))
                    }
                })
                .collect();

            let headers = headers
                .into_iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect::<Vec<String>>()
                .join("\r\n");
            block.push_str(&headers);
            block.push_str("\r\n");

            body.put(block.as_bytes());
        }
        body.put(format!("--{}--", boundary).as_bytes());

        let content_length = body.len();
        let body = AsyncBody::Bytes(body.freeze());

        let mut req = req_builder
            .header(CONTENT_LENGTH, content_length)
            .body(body)
            .map_err(new_request_build_error)?;
        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }
}

fn batch_del_resp_parse(
    boundary: &str,
    body: String,
    expect: Vec<String>,
) -> Result<Vec<(String, Result<RpDelete>)>> {
    let mut reps = Vec::with_capacity(expect.len());

    let mut resp_packs: Vec<&str> = body.trim().split(&format!("--{boundary}")).collect();
    if resp_packs.len() != (expect.len() + 2) {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "invalid batch delete response",
        ));
    }
    // drop the tail
    resp_packs.pop();
    for (resp_pack, name) in resp_packs[1..].iter().zip(expect.into_iter()) {
        // the http body use CRLF (\r\n) instead of LF (\n)
        // split the body at double CRLF
        let split: Vec<&str> = resp_pack.split("\r\n\r\n").collect();

        let header: Vec<&str> = split
            .get(1)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Empty item in batch response"))?
            .trim()
            .split_ascii_whitespace()
            .collect();

        let status_code = header
            .get(1)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "cannot find status code of HTTP response item!",
                )
            })?
            .parse::<u16>()
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    &format!("invalid status code: {:?}", e),
                )
            })?
            .try_into()
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    &format!("invalid status code: {:?}", e),
                )
            })?;

        let rep = match status_code {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => (name, Ok(RpDelete::default())),
            s => {
                let body = split.get(1).ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "Empty HTTP error response")
                })?;
                let err = parse_http_error(s, body)?;
                (name, Err(err))
            }
        };
        reps.push(rep)
    }
    Ok(reps)
}

#[cfg(test)]
mod tests {
    use crate::{
        services::azblob::backend::{batch_del_resp_parse, infer_storage_name_from_endpoint},
        Builder,
    };

    use super::AzblobBuilder;

    #[test]
    fn test_infer_storage_name_from_endpoint() {
        let endpoint = "https://account.blob.core.windows.net";
        let storage_name = infer_storage_name_from_endpoint(endpoint);
        assert_eq!(storage_name, Some("account".to_string()));
    }

    #[test]
    fn test_infer_storage_name_from_endpoint_with_trailing_slash() {
        let endpoint = "https://account.blob.core.windows.net/";
        let storage_name = infer_storage_name_from_endpoint(endpoint);
        assert_eq!(storage_name, Some("account".to_string()));
    }

    #[test]
    fn test_builder_from_endpoint_and_key_infer_account_name() {
        let mut azblob_builder = AzblobBuilder::default();
        azblob_builder.endpoint("https://storagesample.blob.core.chinacloudapi.cn");
        azblob_builder.container("container");
        azblob_builder.account_key("account-key");
        let azblob = azblob_builder
            .build()
            .expect("build azblob should be succeeded.");

        assert_eq!(
            azblob.endpoint,
            "https://storagesample.blob.core.chinacloudapi.cn"
        );

        assert_eq!(azblob._account_name, "storagesample".to_string());

        assert_eq!(azblob.container, "container".to_string());

        assert_eq!(
            azblob_builder.account_key.unwrap(),
            "account-key".to_string()
        );
    }

    #[test]
    fn test_no_key_wont_infer_account_name() {
        let mut azblob_builder = AzblobBuilder::default();
        azblob_builder.endpoint("https://storagesample.blob.core.windows.net");
        azblob_builder.container("container");
        let azblob = azblob_builder
            .build()
            .expect("build azblob should be succeeded.");

        assert_eq!(
            azblob.endpoint,
            "https://storagesample.blob.core.windows.net"
        );

        assert_eq!(azblob._account_name, "".to_string());

        assert_eq!(azblob.container, "container".to_string());

        assert_eq!(azblob_builder.account_key, None);
    }

    #[test]
    fn test_builder_from_endpoint_and_sas() {
        let mut azblob_builder = AzblobBuilder::default();
        azblob_builder.endpoint("https://storagesample.blob.core.usgovcloudapi.net");
        azblob_builder.container("container");
        azblob_builder.account_name("storagesample");
        azblob_builder.account_key("account-key");
        azblob_builder.sas_token("sas");
        let azblob = azblob_builder
            .build()
            .expect("build azblob should be succeeded.");

        assert_eq!(
            azblob.endpoint,
            "https://storagesample.blob.core.usgovcloudapi.net"
        );

        assert_eq!(azblob._account_name, "storagesample".to_string());

        assert_eq!(azblob.container, "container".to_string());

        assert_eq!(
            azblob_builder.account_key.unwrap(),
            "account-key".to_string()
        );

        assert_eq!(azblob_builder.sas_token.unwrap(), "sas".to_string());
    }

    #[test]
    fn test_builder_from_connection_string() {
        let builder = AzblobBuilder::from_connection_string(
            r#"
DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;
AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;
QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;
TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
        "#,
        )
        .expect("from connection string must succeed");

        assert_eq!(
            builder.endpoint.unwrap(),
            "http://127.0.0.1:10000/devstoreaccount1"
        );
        assert_eq!(builder.account_name.unwrap(), "devstoreaccount1");
        assert_eq!(builder.account_key.unwrap(), "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");

        let builder = AzblobBuilder::from_connection_string(
            r#"
DefaultEndpointsProtocol=https;
AccountName=storagesample;
AccountKey=account-key;
EndpointSuffix=core.chinacloudapi.cn;
        "#,
        )
        .expect("from connection string must succeed");

        assert_eq!(
            builder.endpoint.unwrap(),
            "https://storagesample.blob.core.chinacloudapi.cn"
        );
        assert_eq!(builder.account_name.unwrap(), "storagesample");
        assert_eq!(builder.account_key.unwrap(), "account-key")
    }

    #[test]
    fn test_sas_from_connection_string() {
        // Note, not a correct HMAC
        let builder = AzblobBuilder::from_connection_string(
            r#"
BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;
QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;
TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
SharedAccessSignature=sv=2021-01-01&ss=b&srt=c&sp=rwdlaciytfx&se=2022-01-01T11:00:14Z&st=2022-01-02T03:00:14Z&spr=https&sig=KEllk4N8f7rJfLjQCmikL2fRVt%2B%2Bl73UBkbgH%2FK3VGE%3D
        "#,
        )
        .expect("from connection string must succeed");

        assert_eq!(
            builder.endpoint.unwrap(),
            "http://127.0.0.1:10000/devstoreaccount1"
        );
        assert_eq!(builder.sas_token.unwrap(), "sv=2021-01-01&ss=b&srt=c&sp=rwdlaciytfx&se=2022-01-01T11:00:14Z&st=2022-01-02T03:00:14Z&spr=https&sig=KEllk4N8f7rJfLjQCmikL2fRVt%2B%2Bl73UBkbgH%2FK3VGE%3D");
        assert_eq!(builder.account_name, None);
        assert_eq!(builder.account_key, None);
    }

    #[test]
    pub fn test_sas_preferred() {
        let builder = AzblobBuilder::from_connection_string(
            r#"
BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;
AccountName=storagesample;
AccountKey=account-key;
SharedAccessSignature=sv=2021-01-01&ss=b&srt=c&sp=rwdlaciytfx&se=2022-01-01T11:00:14Z&st=2022-01-02T03:00:14Z&spr=https&sig=KEllk4N8f7rJfLjQCmikL2fRVt%2B%2Bl73UBkbgH%2FK3VGE%3D
        "#,
        )
        .expect("from connection string must succeed");

        // SAS should be preferred over shared key
        assert_eq!(builder.sas_token.unwrap(), "sv=2021-01-01&ss=b&srt=c&sp=rwdlaciytfx&se=2022-01-01T11:00:14Z&st=2022-01-02T03:00:14Z&spr=https&sig=KEllk4N8f7rJfLjQCmikL2fRVt%2B%2Bl73UBkbgH%2FK3VGE%3D");
        assert_eq!(builder.account_name, None);
        assert_eq!(builder.account_key, None);
    }

    #[test]
    fn test_break_down_batch() {
        // the last item in batch is a mocked response.
        // if stronger validation is implemented for Azblob,
        // feel free to replace or remove it.
        let body = r#"--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed
Content-Type: application/http
Content-ID: 0

HTTP/1.1 202 Accepted
x-ms-delete-type-permanent: true
x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e284f
x-ms-version: 2018-11-09

--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed
Content-Type: application/http
Content-ID: 1

HTTP/1.1 202 Accepted
x-ms-delete-type-permanent: true
x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2851
x-ms-version: 2018-11-09

--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed
Content-Type: application/http
Content-ID: 2

HTTP/1.1 404 The specified blob does not exist.
x-ms-error-code: BlobNotFound
x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2852
x-ms-version: 2018-11-09
Content-Length: 216
Content-Type: application/xml

<?xml version="1.0" encoding="utf-8"?>
<Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.
RequestId:778fdc83-801e-0000-62ff-0334671e2852
Time:2018-06-14T16:46:54.6040685Z</Message></Error>

--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed
Content-Type: application/http
Content-ID: 3

HTTP/1.1 403 Request to blob forbidden
x-ms-error-code: BlobForbidden
x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2852
x-ms-version: 2018-11-09
Content-Length: 216
Content-Type: application/xml

<?xml version="1.0" encoding="utf-8"?>
<Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.
RequestId:778fdc83-801e-0000-62ff-0334671e2852
Time:2018-06-14T16:46:54.6040685Z</Message></Error>
--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed--"#
            .replace('\n', "\r\n");

        let expected: Vec<_> = (0..=3).map(|n| format!("/to-del/{n}")).collect();
        let boundary = "batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed";
        let p = batch_del_resp_parse(boundary, body, expected.clone()).expect("must_success");
        assert_eq!(p.len(), expected.len());
        for (idx, ((del, rep), to_del)) in p.into_iter().zip(expected.into_iter()).enumerate() {
            assert_eq!(del, to_del);

            if idx != 3 {
                assert!(rep.is_ok());
            } else {
                assert!(rep.is_err());
            }
        }
    }
}
