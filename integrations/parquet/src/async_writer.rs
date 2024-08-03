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

use bytes::Bytes;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::{ParquetError, Result};

use futures::future::BoxFuture;
use opendal::Writer;

/// OpendalAsyncWriter implements AsyncFileWriter trait by using opendal.
///
/// ```no_run
/// use parquet::arrow::async_writer::AsyncFileWriter;
/// use parquet::OpendalAsyncWriter;
/// use opendal::services::S3;
/// use opendal::{Builder, Operator};
///
/// #[tokio::main]
/// async fn main() {
///     let builder = S3::from_map(
///         vec![
///             ("access_key".to_string(), "my_access_key".to_string()),
///             ("secret_key".to_string(), "my_secret_key".to_string()),
///             ("endpoint".to_string(), "my_endpoint".to_string()),
///             ("region".to_string(), "my_region".to_string()),
///         ]
///         .into_iter()
///         .collect(),
///     ).unwrap();
///
///     // Create a new operator
///     let operator = Operator::new(builder).unwrap().finish();
///     let path = "/path/to/file.parquet";
///     // Create a new object store
///     let mut writer = Arc::new(OpendalAsyncWriter::new(operator.writer(path)));
/// }
/// ```
pub struct OpendalAsyncWriter {
    inner: Writer,
}

impl OpendalAsyncWriter {
    /// Create a [`OpendalAsyncWriter`] by given [`Writer`].
    pub fn new(writer: Writer) -> Self {
        Self { inner: writer }
    }
}

impl AsyncFileWriter for OpendalAsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.inner
                .write(bs)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.inner
                .close()
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::{services, Operator};

    #[tokio::test]
    async fn test_basic() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "data/test.txt";
        let mut writer = OpendalAsyncWriter::new(op.writer(path).await.unwrap());
        let bytes = Bytes::from_static(b"hello, world!");
        writer.write(bytes).await.unwrap();
        let bytes = Bytes::from_static(b"hello, OpenDAL!");
        writer.write(bytes).await.unwrap();
        writer.complete().await.unwrap();

        let bytes = op.read(path).await.unwrap().to_vec();
        assert_eq!(bytes, b"hello, world!hello, OpenDAL!");
    }

    #[tokio::test]
    async fn test_abort() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "data/test.txt";
        let mut writer = OpendalAsyncWriter::new(op.writer(path).await.unwrap());
        let bytes = Bytes::from_static(b"hello, world!");
        writer.write(bytes).await.unwrap();
        let bytes = Bytes::from_static(b"hello, OpenDAL!");
        writer.write(bytes).await.unwrap();
        drop(writer);

        let exist = op.is_exist(path).await.unwrap();
        assert!(!exist);
    }
}
