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

use std::cmp::{max, min};
use std::ops::Range;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use opendal::Reader;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

const PREFETCH_FOOTER_SIZE: usize = 512 * 1024;

/// AsyncReader implements AsyncFileReader trait by using opendal.
///
/// ```no_run
/// use std::sync::Arc;
/// use arrow::array::{ArrayRef, Int64Array, RecordBatch};
///
/// use futures::StreamExt;
/// use opendal::{services::S3Config, Operator};
/// use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
/// use parquet_opendal::{AsyncReader, AsyncWriter};
///
/// #[tokio::main]
/// async fn main() {
///     let mut cfg = S3Config::default();
///     cfg.access_key_id = Some("my_access_key".to_string());
///     cfg.secret_access_key = Some("my_secret_key".to_string());
///     cfg.endpoint = Some("my_endpoint".to_string());
///     cfg.region = Some("my_region".to_string());
///     cfg.bucket = "my_bucket".to_string();
///
///     // Create a new operator
///     let operator = Operator::from_config(cfg).unwrap();
///     let path = "/path/to/file.parquet";
///
///     // Create an async writer
///     let writer = AsyncWriter::new(
///         operator
///             .writer_with(path)
///             .chunk(32 * 1024 * 1024)
///             .concurrent(8)
///             .await
///             .unwrap(),
///     );
///
///     let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
///     let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
///     let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), None).unwrap();
///     writer.write(&to_write).await.unwrap();
///     writer.close().await.unwrap();
///
///     // gap: Allow the underlying reader to merge small IOs
///     // when the gap between multiple IO ranges is less than the threshold.
///     let reader = operator
///         .reader_with(path)
///         .gap(512 * 1024)
///         .chunk(16 * 1024 * 1024)
///         .concurrent(16)
///         .await
///         .unwrap();
///     let content_len = operator.stat(path).await.unwrap().content_length();
///     let reader = AsyncReader::new(reader, content_len).with_prefetch_footer_size(512 * 1024);
///     let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
///         .await
///         .unwrap()
///         .build()
///         .unwrap();
///     let read = stream.next().await.unwrap().unwrap();
///     assert_eq!(to_write, read);
/// }
/// ```
pub struct AsyncReader {
    inner: Reader,
    content_length: u64,
    // The prefetch size for fetching file footer.
    prefetch_footer_size: usize,
    // Whether to load the column index as part of get_metadata.
    preload_column_index: bool,
    // Whether to load the offset index as part of get_metadata.
    preload_offset_index: bool,
}

fn set_prefetch_footer_size(footer_size: usize, content_size: u64) -> usize {
    let footer_size = max(footer_size, FOOTER_SIZE);
    min(footer_size as u64, content_size) as usize
}

impl AsyncReader {
    /// Create a [`AsyncReader`] by given [`Reader`].
    pub fn new(reader: Reader, content_length: u64) -> Self {
        Self {
            inner: reader,
            content_length,
            prefetch_footer_size: set_prefetch_footer_size(PREFETCH_FOOTER_SIZE, content_length),
            preload_column_index: false,
            preload_offset_index: false,
        }
    }

    /// Set prefetch size for fetching file footer.
    pub fn with_prefetch_footer_size(mut self, footer_size: usize) -> Self {
        self.prefetch_footer_size = set_prefetch_footer_size(footer_size, self.content_length);
        self
    }

    /// Whether to load the Column Index as part of `get_metadata`
    ///
    /// Note: This setting may be overridden by [`ArrowReaderOptions`] `page_index_policy`.
    /// If `page_index_policy` is `Optional` or `Required`, it will take precedence
    /// over this preload flag. When it is `Skip` (default), this flag is used.
    pub fn with_preload_column_index(mut self, preload_column_index: bool) -> Self {
        self.preload_column_index = preload_column_index;
        self
    }

    /// Whether to load the Offset Index as part of `get_metadata`
    ///
    /// Note: This setting may be overridden by [`ArrowReaderOptions`] `page_index_policy`.
    /// If `page_index_policy` is `Optional` or `Required`, it will take precedence
    /// over this preload flag. When it is `Skip` (default), this flag is used.
    pub fn with_preload_offset_index(mut self, preload_offset_index: bool) -> Self {
        self.preload_offset_index = preload_offset_index;
        self
    }
}

impl AsyncFileReader for AsyncReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
        async move {
            Ok(self
                .inner
                .read(range)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?
                .to_bytes())
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<bytes::Bytes>>> {
        async move {
            Ok(self
                .inner
                .fetch(ranges)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?
                .into_iter()
                .map(|buf| buf.to_bytes())
                .collect::<Vec<_>>())
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<std::sync::Arc<ParquetMetaData>>> {
        let content_length = self.content_length;
        let prefetch_footer_size = self.prefetch_footer_size;
        let preload_column_index = self.preload_column_index;
        let preload_offset_index = self.preload_offset_index;
        async move {
            // `with_arrow_reader_options` keeps the preload flags when `options`
            // does not request the page index and forwards decryption and other
            // metadata options when it does.
            let reader = ParquetMetaDataReader::new()
                .with_prefetch_hint(Some(prefetch_footer_size))
                .with_column_index_policy(PageIndexPolicy::from(preload_column_index))
                .with_offset_index_policy(PageIndexPolicy::from(preload_offset_index))
                .with_arrow_reader_options(options);
            let meta = reader.load_and_finish(self, content_length).await?;

            Ok(Arc::new(meta))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use opendal::{Operator, services};
    use rand::{RngExt, distr::Alphanumeric};

    use crate::{AsyncReader, AsyncWriter, async_reader::PREFETCH_FOOTER_SIZE};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use parquet::{
        arrow::{
            AsyncArrowWriter, ParquetRecordBatchStreamBuilder, arrow_reader::ArrowReaderOptions,
            async_reader::AsyncFileReader,
        },
        file::metadata::{KeyValue, PageIndexPolicy},
        file::properties::WriterProperties,
    };

    #[tokio::test]
    async fn test_async_reader_with_prefetch_footer_size() {
        let operator = Operator::new(services::Memory::default()).unwrap();
        let path = "/path/to/file.parquet";

        let reader = AsyncReader::new(operator.reader(path).await.unwrap(), 1024);
        assert_eq!(reader.prefetch_footer_size, 1024);
        assert_eq!(reader.content_length, 1024);

        let reader = AsyncReader::new(operator.reader(path).await.unwrap(), 1024 * 1024);
        assert_eq!(reader.prefetch_footer_size, PREFETCH_FOOTER_SIZE);
        assert_eq!(reader.content_length, 1024 * 1024);

        let reader = AsyncReader::new(operator.reader(path).await.unwrap(), 1024 * 1024)
            .with_prefetch_footer_size(2048 * 1024);
        assert_eq!(reader.prefetch_footer_size, 1024 * 1024);
        assert_eq!(reader.content_length, 1024 * 1024);

        let reader = AsyncReader::new(operator.reader(path).await.unwrap(), 1024 * 1024)
            .with_prefetch_footer_size(1);
        assert_eq!(reader.prefetch_footer_size, 8);
        assert_eq!(reader.content_length, 1024 * 1024);
    }

    fn gen_fixed_string(size: usize) -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .map(char::from)
            .collect()
    }

    #[tokio::test]
    async fn test_async_reader() {
        let operator = Operator::new(services::Memory::default()).unwrap();
        let path = "/path/to/file.parquet";
        let writer = AsyncWriter::new(
            operator
                .writer_with(path)
                .chunk(32 * 1024 * 1024)
                .concurrent(8)
                .await
                .unwrap(),
        );

        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
        let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), None).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        let reader = operator.reader(path).await.unwrap();
        let content_len = operator.stat(path).await.unwrap().content_length();
        let reader = AsyncReader::new(reader, content_len);
        let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .build()
            .unwrap();
        let read = stream.next().await.unwrap().unwrap();
        assert_eq!(to_write, read);
    }

    /// Write a small parquet file to `path` on `operator` and return its
    /// content length. `writer_properties` defaults to `None`, which enables
    /// page-level statistics and writes both the column and offset index.
    async fn write_test_file(
        operator: &Operator,
        path: &str,
        writer_properties: Option<WriterProperties>,
    ) -> (RecordBatch, u64) {
        let writer = AsyncWriter::new(operator.writer(path).await.unwrap());
        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
        let mut writer =
            AsyncArrowWriter::try_new(writer, to_write.schema(), writer_properties).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        let content_len = operator.stat(path).await.unwrap().content_length();
        (to_write, content_len)
    }

    async fn open_test_reader(operator: &Operator, path: &str) -> AsyncReader {
        let content_len = operator.stat(path).await.unwrap().content_length();
        AsyncReader::new(operator.reader(path).await.unwrap(), content_len)
    }

    #[tokio::test]
    async fn test_get_metadata_honors_page_index_policy() {
        let operator = Operator::new(services::Memory::default()).unwrap();
        let path = "/path/with_page_index.parquet";
        // Default writer properties write page-level statistics, so the file
        // carries both a column index and an offset index.
        write_test_file(&operator, path, None).await;

        // `PageIndexPolicy::Required` loads and validates the page indexes.
        let mut reader = open_test_reader(&operator, path).await;
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let metadata = reader.get_metadata(Some(&options)).await.unwrap();
        let page_index = metadata.page_index_for_row_group(0);
        assert!(page_index.column_index(0).is_some());
        assert!(page_index.offset_index(0).is_some());

        // `PageIndexPolicy::Optional` loads the page indexes when present.
        let mut reader = open_test_reader(&operator, path).await;
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let metadata = reader.get_metadata(Some(&options)).await.unwrap();
        let page_index = metadata.page_index_for_row_group(0);
        assert!(page_index.column_index(0).is_some());
        assert!(page_index.offset_index(0).is_some());

        // `PageIndexPolicy::Skip` (the default) does not load the page indexes,
        // matching the previous `get_metadata(None)` behavior.
        let mut reader = open_test_reader(&operator, path).await;
        let metadata = reader.get_metadata(None).await.unwrap();
        assert!(metadata.page_index().is_none());

        let mut reader = open_test_reader(&operator, path).await;
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
        let metadata = reader.get_metadata(Some(&options)).await.unwrap();
        assert!(metadata.page_index().is_none());
    }

    #[tokio::test]
    async fn test_get_metadata_page_index_required_without_index() {
        let operator = Operator::new(services::Memory::default()).unwrap();
        let path = "/path/without_page_index.parquet";
        // `set_offset_index_disabled` drops the offset index from the file metadata.
        let writer_properties = WriterProperties::builder()
            .set_offset_index_disabled(true)
            .build();
        write_test_file(&operator, path, Some(writer_properties)).await;

        // `PageIndexPolicy::Required` errors when the file has no offset index.
        let mut reader = open_test_reader(&operator, path).await;
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let err = reader.get_metadata(Some(&options)).await.unwrap_err();
        assert!(
            err.to_string().contains("missing offset index"),
            "unexpected error: {err}"
        );

        // `PageIndexPolicy::Optional` succeeds and leaves the offset index empty.
        let mut reader = open_test_reader(&operator, path).await;
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let metadata = reader.get_metadata(Some(&options)).await.unwrap();
        let page_index = metadata.page_index_for_row_group(0);
        assert!(page_index.offset_index(0).is_none());
    }

    #[tokio::test]
    async fn test_get_metadata_preload_page_index() {
        let operator = Operator::new(services::Memory::default()).unwrap();
        let path = "/path/with_page_index.parquet";
        write_test_file(&operator, path, None).await;

        // The preload flags load the indexes even though the options do not
        // request them (`get_metadata(None)` behaves like `Skip`).
        let mut reader = open_test_reader(&operator, path)
            .await
            .with_preload_column_index(true)
            .with_preload_offset_index(true);
        let metadata = reader.get_metadata(None).await.unwrap();
        let page_index = metadata.page_index_for_row_group(0);
        assert!(page_index.column_index(0).is_some());
        assert!(page_index.offset_index(0).is_some());

        // An explicit `Required` policy overrides the preload flags entirely:
        // the offset index policy falls back to `Skip` when the options only
        // request the column index.
        let mut reader = open_test_reader(&operator, path)
            .await
            .with_preload_column_index(true)
            .with_preload_offset_index(true);
        let options = ArrowReaderOptions::new().with_column_index_policy(PageIndexPolicy::Required);
        let metadata = reader.get_metadata(Some(&options)).await.unwrap();
        let page_index = metadata.page_index_for_row_group(0);
        assert!(page_index.column_index(0).is_some());
        assert!(page_index.offset_index(0).is_none());
    }

    #[tokio::test]
    async fn test_stream_builder_with_options_loads_page_index() {
        let operator = Operator::new(services::Memory::default()).unwrap();
        let path = "/path/with_page_index.parquet";
        let (to_write, _) = write_test_file(&operator, path, None).await;

        let reader = open_test_reader(&operator, path).await;
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let stream = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
            .await
            .unwrap();
        let page_index = stream.metadata().page_index_for_row_group(0);
        assert!(page_index.column_index(0).is_some());
        assert!(page_index.offset_index(0).is_some());

        let mut stream = stream.build().unwrap();
        let read = stream.next().await.unwrap().unwrap();
        assert_eq!(to_write, read);
    }

    struct TestCase {
        metadata_size: usize,
        prefetch: Option<usize>,
    }

    #[tokio::test]
    async fn test_async_reader_with_large_metadata() {
        for case in [
            TestCase {
                metadata_size: 256 * 1024,
                prefetch: None,
            },
            TestCase {
                metadata_size: 1024 * 1024,
                prefetch: None,
            },
            TestCase {
                metadata_size: 256 * 1024,
                prefetch: Some(4),
            },
            TestCase {
                metadata_size: 1024 * 1024,
                prefetch: Some(4),
            },
        ] {
            let operator = Operator::new(services::Memory::default()).unwrap();
            let path = "/path/to/file.parquet";
            let writer = AsyncWriter::new(
                operator
                    .writer_with(path)
                    .chunk(32 * 1024 * 1024)
                    .concurrent(8)
                    .await
                    .unwrap(),
            );

            let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
            let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

            let mut writer = AsyncArrowWriter::try_new(
                writer,
                to_write.schema(),
                Some(
                    WriterProperties::builder()
                        .set_key_value_metadata(Some(vec![KeyValue {
                            key: "__metadata".to_string(),
                            value: Some(gen_fixed_string(case.metadata_size)),
                        }]))
                        .build(),
                ),
            )
            .unwrap();
            writer.write(&to_write).await.unwrap();
            writer.close().await.unwrap();

            let reader = operator.reader(path).await.unwrap();
            let content_len = operator.stat(path).await.unwrap().content_length();
            let mut reader = AsyncReader::new(reader, content_len);
            if let Some(footer_size) = case.prefetch {
                reader = reader.with_prefetch_footer_size(footer_size);
            }
            let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .unwrap()
                .build()
                .unwrap();
            let read = stream.next().await.unwrap().unwrap();
            assert_eq!(to_write, read);
        }
    }
}
