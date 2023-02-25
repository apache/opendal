// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::mem;
use std::sync::Arc;

use async_trait::async_trait;
use http::header::HeaderName;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use reqsign::AzureStorageSigner;

use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::object::ObjectMetadata;
use crate::ops::*;
use crate::raw::*;
use crate::*;

const X_MS_BLOB_TYPE: &str = "x-ms-blob-type";

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
/// - [ ] multipart
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
/// use opendal::Object;
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
///     let op: Operator = Operator::create(builder)?.finish();
///
///     // Create an object handle to start operation on object.
///     let _: Object = op.object("test_file");
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
                    ErrorKind::BackendConfigInvalid,
                    "connection string must have AccountName",
                )
                .with_operation("Builder::from_connection_string")
            })?;
            builder.account_name(account_name);
            let account_key = conn_map.get("AccountKey").ok_or_else(|| {
                Error::new(
                    ErrorKind::BackendConfigInvalid,
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
                        ErrorKind::BackendConfigInvalid,
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
            true => Err(
                Error::new(ErrorKind::BackendConfigInvalid, "container is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Azblob),
            ),
        }?;
        debug!("backend use container {}", &container);

        let endpoint = match &self.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(
                Error::new(ErrorKind::BackendConfigInvalid, "endpoint is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::Azblob),
            ),
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
        if let Some(sas_token) = &self.sas_token {
            signer_builder.security_token(sas_token);
        } else if let (Some(name), Some(key)) = (&self.account_name, &self.account_key) {
            signer_builder.account_name(name).account_key(key);
        }

        let signer = signer_builder.build().map_err(|e| {
            Error::new(ErrorKind::BackendConfigInvalid, "build AzureStorageSigner")
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
            container: self.container.clone(),
            client,
            _account_name: mem::take(&mut self.account_name).unwrap_or_default(),
        })
    }
}

/// Backend for azblob services.
#[derive(Debug, Clone)]
pub struct AzblobBackend {
    container: String,
    client: HttpClient,
    root: String, // root will be "/" or /abc/
    endpoint: String,
    signer: Arc<AzureStorageSigner>,
    _account_name: String,
}

#[async_trait]
impl Accessor for AzblobBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Pager = DirStream;
    type BlockingPager = ();

    fn metadata(&self) -> AccessorMetadata {
        use AccessorCapability::*;
        use AccessorHint::*;

        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Azblob)
            .set_root(&self.root)
            .set_name(&self.container)
            .set_capabilities(Read | Write | List | Scan)
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
                let meta = parse_into_object_metadata(path, resp.headers())?;

                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        let mut req = self.azblob_put_blob_request(
            path,
            Some(args.size()),
            args.content_type(),
            AsyncBody::Reader(r),
        )?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        let resp = self.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpWrite::new(args.size()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)));
        }

        let resp = self.azblob_get_blob_properties(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_object_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)))
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
        let op = DirStream::new(
            Arc::new(self.clone()),
            self.root.clone(),
            path.to_string(),
            "/".to_string(),
            args.limit(),
        );

        Ok((RpList::default(), op))
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        let op = DirStream::new(
            Arc::new(self.clone()),
            self.root.clone(),
            path.to_string(),
            "".to_string(),
            args.limit(),
        );

        Ok((RpScan::default(), op))
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

    fn azblob_put_blob_request(
        &self,
        path: &str,
        size: Option<u64>,
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
}

#[cfg(test)]
mod tests {
    use super::AzblobBuilder;

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
}
