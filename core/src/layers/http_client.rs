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

use crate::raw::*;
use crate::*;

/// Layer for replacing the default HTTP client with a custom one.
///
/// This layer allows users to provide their own HTTP client implementation
/// by implementing the [`HttpFetch`] trait. This is useful when you need
/// to customize HTTP behavior, add authentication, use a different HTTP
/// client library, or apply custom middleware.
///
/// # Examples
///
/// ```no_run
/// use opendal::layers::HttpClientLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Result;
/// use opendal::raw::HttpClient;
///
/// # fn main() -> Result<()> {
/// // Create a custom HTTP client
/// let custom_client = HttpClient::new()?;
///
/// let op = Operator::new(services::S3::default())?
///     .layer(HttpClientLayer::new(custom_client))
///     .finish();
/// # Ok(())
/// # }
/// ```
///
/// # Custom HTTP Client Implementation
///
/// ```no_run
/// use opendal::raw::{HttpFetch, HttpBody, Buffer};
/// use http::{Request, Response};
/// use opendal::Result;
///
/// struct CustomHttpClient {
///     // Your custom HTTP client fields
/// }
///
/// impl HttpFetch for CustomHttpClient {
///     async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
///         // Your custom HTTP client implementation
///         todo!()
///     }
/// }
/// ```
#[derive(Clone)]
pub struct HttpClientLayer {
    client: HttpClient,
}

impl HttpClientLayer {
    /// Create a new `HttpClientLayer` with the given HTTP client.
    ///
    /// # Arguments
    ///
    /// * `client` - The HTTP client to use for all HTTP requests
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use opendal::layers::HttpClientLayer;
    /// use opendal::raw::HttpClient;
    /// use opendal::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let client = HttpClient::new()?;
    /// let layer = HttpClientLayer::new(client);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(client: HttpClient) -> Self {
        Self { client }
    }
}

impl<A: Access> Layer<A> for HttpClientLayer {
    type LayeredAccess = HttpClientAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();

        // Replace the HTTP client with our custom one
        info.update_http_client(|_| self.client.clone());

        HttpClientAccessor { inner }
    }
}

/// The accessor returned by [`HttpClientLayer`].
///
/// This accessor simply passes through all operations to the inner accessor,
/// while the HTTP client replacement is handled at the layer level.
#[derive(Debug, Clone)]
pub struct HttpClientAccessor<A: Access> {
    inner: A,
}

impl<A: Access> LayeredAccess for HttpClientAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner.copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner.rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services;

    #[tokio::test]
    async fn test_http_client_layer() {
        let layer = HttpClientLayer::new(HttpClient::new().unwrap());
        let op = Operator::new(services::Memory::default())
            .unwrap()
            .layer(layer)
            .finish();

        // Basic test to ensure the layer doesn't break functionality
        op.write("test", "data").await.unwrap();
        let content = op.read("test").await.unwrap();
        assert_eq!(content.to_bytes(), "data");
    }

    #[tokio::test]
    async fn test_http_client_layer_with_fetcher() {
        struct MockFetcher;

        impl HttpFetch for MockFetcher {
            async fn fetch(&self, _req: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
                // For testing purposes, we just return an error since Memory service doesn't use HTTP
                Err(Error::new(ErrorKind::Unexpected, "mock fetcher"))
            }
        }

        let client = HttpClient::with(MockFetcher);
        let layer = HttpClientLayer::new(client);
        let _op = Operator::new(services::Memory::default())
            .unwrap()
            .layer(layer)
            .finish();

        // The layer should be created successfully even with a custom fetcher
    }
}
