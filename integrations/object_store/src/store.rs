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

use std::fmt::{self, Debug, Display, Formatter};
use std::future::IntoFuture;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use crate::datetime_to_timestamp;
use crate::utils::*;
use async_trait::async_trait;
use asyncband::mutex::Mutex;
use asyncband::oneshot;
use bytes::Bytes;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::Attribute;
use object_store::Attributes;
use object_store::CopyMode as ObjectStoreCopyMode;
use object_store::CopyOptions as ObjectStoreCopyOptions;
use object_store::Extensions;
use object_store::ListResult;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutMultipartOptions;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::PutResult;
use object_store::RenameOptions;
use object_store::RenameTargetMode;
use object_store::path::Path;
use object_store::{GetOptions, UploadPart};
use object_store::{GetRange, GetResultPayload};
use object_store::{GetResult, PutMode};
use opendal::Buffer;
use opendal::BytesRange;
use opendal::ErrorKind;
use opendal::FuturesBytesStream;
use opendal::Writer;
use opendal::options::CopyOptions;
use opendal::options::ReaderOptions;
use opendal::options::StatOptions;
use opendal::raw::normalize_path;
use opendal::raw::percent_decode_path;
use opendal::{Operator, OperatorInfo};
use std::collections::HashMap;

const DEFAULT_CONCURRENT: usize = 8;
const DEFAULT_GET_RANGES_GAP: usize = object_store::OBJECT_STORE_COALESCE_DEFAULT as usize;

fn format_object_attributes(meta: &opendal::Metadata) -> Attributes {
    let mut attributes = Attributes::new();
    for (attribute, value) in [
        (Attribute::CacheControl, meta.cache_control()),
        (Attribute::ContentDisposition, meta.content_disposition()),
        (Attribute::ContentEncoding, meta.content_encoding()),
        (Attribute::ContentLanguage, meta.content_language()),
        (Attribute::ContentType, meta.content_type()),
    ] {
        if let Some(value) = value {
            attributes.insert(attribute, value.to_owned().into());
        }
    }
    if let Some(user_meta) = meta.user_metadata() {
        for (key, value) in user_meta {
            attributes.insert(
                Attribute::Metadata(key.to_string().into()),
                value.to_string().into(),
            );
        }
    }

    attributes
}

fn format_reader_options(options: &GetOptions, content_length_hint: Option<u64>) -> ReaderOptions {
    ReaderOptions {
        version: options.version.clone(),
        if_match: options.if_match.clone(),
        if_none_match: options.if_none_match.clone(),
        if_modified_since: options.if_modified_since.and_then(datetime_to_timestamp),
        if_unmodified_since: options.if_unmodified_since.and_then(datetime_to_timestamp),
        content_length_hint,
        ..Default::default()
    }
}

fn format_stat_options(options: &GetOptions) -> StatOptions {
    StatOptions {
        version: options.version.clone(),
        if_match: options.if_match.clone(),
        if_none_match: options.if_none_match.clone(),
        if_modified_since: options.if_modified_since.and_then(datetime_to_timestamp),
        if_unmodified_since: options.if_unmodified_since.and_then(datetime_to_timestamp),
        ..Default::default()
    }
}

fn format_read_range(range: Option<&GetRange>, size: u64) -> opendal::Result<Range<u64>> {
    match range {
        Some(range) => range
            .as_range(size)
            .map_err(|err| opendal::Error::new(ErrorKind::RangeNotSatisfied, err.to_string())),
        None => Ok(0..size),
    }
}

/// OpendalStore implements ObjectStore trait by using opendal.
///
/// This allows users to use opendal as an object store without extra cost.
///
/// Paths are resolved against the operator's configured root; see
/// [`OpendalStore::new`] for the path resolution convention and a runnable
/// example.
///
/// Visit [`opendal::services`] for more information about supported services.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use bytes::Bytes;
/// use object_store::path::Path;
/// use object_store::ObjectStore;
/// use object_store::ObjectStoreExt;
/// use object_store_opendal::OpendalStore;
/// use opendal::services::S3;
/// use opendal::{Builder, Operator};
///
/// #[tokio::main]
/// async fn main() {
///    let builder = S3::default()
///     .access_key_id("my_access_key")
///     .secret_access_key("my_secret_key")
///     .endpoint("my_endpoint")
///     .region("my_region");
///
///     // Create a new operator
///     let operator = Operator::new(builder).unwrap();
///
///     // Create a new object store
///     let object_store = Arc::new(OpendalStore::new(operator));
///
///     let path = Path::from("data/nested/test.txt");
///     let bytes = Bytes::from_static(b"hello, world! I am nested.");
///
///     object_store.put(&path, bytes.clone().into()).await.unwrap();
///
///     let content = object_store
///         .get(&path)
///         .await
///         .unwrap()
///         .bytes()
///         .await
///         .unwrap();
///
///     assert_eq!(content, bytes);
/// }
/// ```
#[derive(Clone)]
pub struct OpendalStore {
    info: Arc<OperatorInfo>,
    inner: Operator,
    get_ranges_gap: usize,
    multipart_chunk_size: Option<usize>,
    multipart_concurrency: usize,
}

impl OpendalStore {
    /// Create OpendalStore by given Operator.
    ///
    /// # Path resolution
    ///
    /// `OpendalStore` forwards every incoming [`object_store::path::Path`] to
    /// the operator, which resolves it against its configured `root`: a store
    /// path `data/test.txt` reaches the service as `<root>/data/test.txt`.
    /// `OpendalStore` never strips a leading prefix, because any prefix can be
    /// a legitimate object key inside the root.
    ///
    /// This matters when the store is registered with a URL-based registry
    /// such as DataFusion's `ObjectStoreRegistry`. The registry key contains
    /// only the registered URL's scheme and authority, so the store receives
    /// the full path of each queried URL. If the same prefix is configured in
    /// both the operator `root` and the queried URLs, lookups fail with a
    /// doubled prefix, as reported in [apache/opendal#7786]. Choose the
    /// single place that owns the prefix:
    ///
    /// - Leave the operator `root` unset so queried URLs carry the full path
    ///   to each object, as `s3://bucket/key` does.
    /// - Set `root` to the shared prefix and register the store under a URL
    ///   whose queried paths are relative to it. For services such as `hf://`
    ///   whose authority does not encode the bucket or repository, configure
    ///   the repository on the builder and register a dedicated authority
    ///   (for example `hf://myrepo`) so that `hf://myrepo/file` hands the
    ///   store a repository-relative `file`.
    ///
    /// The following example uses the memory service but applies to any
    /// service. The memory scheme has no meaningful authority, so the
    /// `memory://my-store` registration below only fixes the URL prefix used
    /// for lookups; the operator `root` owns the `base/dir` prefix.
    ///
    /// [apache/opendal#7786]: https://github.com/apache/opendal/issues/7786
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use bytes::Bytes;
    /// use object_store::path::Path;
    /// use object_store::{ObjectStore, ObjectStoreExt};
    /// use object_store_opendal::OpendalStore;
    /// use opendal::services::Memory;
    /// use opendal::Operator;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     // The operator owns the `base/dir` prefix through its root.
    ///     let op = Operator::new(Memory::default().root("/base/dir")).unwrap();
    ///     let store = OpendalStore::new(op.clone());
    ///
    ///     // Paths passed to the store are store-relative keys: the service
    ///     // sees them resolved against `/base/dir`.
    ///     let path = Path::from("data/test.txt");
    ///     store
    ///         .put(&path, Bytes::from_static(b"hello").into())
    ///         .await
    ///         .unwrap();
    ///     assert_eq!(op.read("data/test.txt").await.unwrap().len(), 5);
    ///
    ///     // A URL-based registry hands the store the path that follows the
    ///     // authority. Registering `memory://my-store` makes
    ///     // `memory://my-store/data/test.txt` reach this store as the same
    ///     // store-relative `data/test.txt`.
    ///     let content = store.get(&path).await.unwrap().bytes().await.unwrap();
    ///     assert_eq!(content, Bytes::from_static(b"hello"));
    ///
    ///     // Repeating the root in the queried URL path looks up
    ///     // `/base/dir/base/dir/data/test.txt`: prefixes are never stripped.
    ///     let doubled = Path::from("base/dir/data/test.txt");
    ///     assert!(matches!(
    ///         store.head(&doubled).await,
    ///         Err(object_store::Error::NotFound { .. })
    ///     ));
    /// }
    /// ```
    pub fn new(op: Operator) -> Self {
        Self {
            info: op.info().into(),
            inner: op,
            get_ranges_gap: DEFAULT_GET_RANGES_GAP,
            multipart_chunk_size: None,
            multipart_concurrency: DEFAULT_CONCURRENT,
        }
    }

    /// Set the maximum gap that [`ObjectStore::get_ranges`] coalesces.
    ///
    /// The default is [`object_store::OBJECT_STORE_COALESCE_DEFAULT`] (1 MiB).
    /// Set this value to `0` to avoid merging ranges separated by any bytes.
    /// Overlapping or adjacent ranges are still merged without over-fetching.
    pub fn with_get_ranges_gap(mut self, gap: usize) -> Self {
        self.get_ranges_gap = gap;
        self
    }

    /// Set the buffered chunk size, in bytes, for multipart uploads.
    ///
    /// By default, OpenDAL chooses the chunk size from the service's capabilities.
    /// This setting applies to [`ObjectStore::put_multipart_opts`] and the
    /// convenience multipart APIs, but not to [`ObjectStore::put_opts`].
    /// OpenDAL clamps the size to the service's minimum and maximum multipart
    /// sizes. The final chunk may be smaller. Upload concurrency is unchanged.
    /// Larger chunks reduce write requests but increase buffering and the amount
    /// of data retransmitted when a request fails.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is zero.
    ///
    /// ```
    /// # use object_store_opendal::OpendalStore;
    /// # use opendal::{Operator, services::Memory};
    /// # let operator = Operator::new(Memory::default()).unwrap();
    /// let store = OpendalStore::new(operator)
    ///     .with_multipart_chunk_size(16 * 1024 * 1024)
    ///     .with_multipart_concurrency(4);
    /// ```
    pub fn with_multipart_chunk_size(mut self, chunk_size: usize) -> Self {
        assert!(
            chunk_size > 0,
            "multipart chunk size must be greater than zero"
        );
        self.multipart_chunk_size = Some(chunk_size);
        self
    }

    /// Set the maximum concurrent writes for each multipart upload.
    ///
    /// Defaults to 8. Set to 1 for sequential writes. Higher values can improve
    /// throughput at the cost of more buffered memory. Services that do not
    /// support concurrent writes continue to write sequentially.
    /// This setting does not affect ordinary puts or reads.
    ///
    /// # Panics
    ///
    /// Panics if `concurrency` is zero.
    pub fn with_multipart_concurrency(mut self, concurrency: usize) -> Self {
        assert!(
            concurrency > 0,
            "multipart concurrency must be greater than zero"
        );
        self.multipart_concurrency = concurrency;
        self
    }

    /// Get the Operator info.
    pub fn info(&self) -> &OperatorInfo {
        self.info.as_ref()
    }

    async fn format_get_error(
        &self,
        location: &Path,
        options: &GetOptions,
        err: opendal::Error,
    ) -> object_store::Error {
        if err.kind() == ErrorKind::ConditionNotMatch
            && (options.if_none_match.is_some() || options.if_modified_since.is_some())
        {
            let not_modified =
                if options.if_match.is_none() && options.if_unmodified_since.is_none() {
                    true
                } else {
                    // OpenDAL reports both classes of failure with one error kind.
                    // Use the current metadata to identify the failed condition
                    // when the caller supplied both classes.
                    let raw_location = percent_decode_path(location.as_ref());
                    let stat_options = StatOptions {
                        version: options.version.clone(),
                        ..Default::default()
                    };
                    self.inner
                        .stat_options(&raw_location, stat_options)
                        .into_send()
                        .await
                        .is_ok_and(|metadata| {
                            let meta = format_object_meta(location.as_ref(), &metadata);
                            matches!(
                                options.check_preconditions(&meta),
                                Err(object_store::Error::NotModified { .. })
                            )
                        })
                };
            if not_modified {
                return object_store::Error::NotModified {
                    path: location.to_string(),
                    source: Box::new(err),
                };
            }
        }
        format_object_store_error(err, location.as_ref())
    }

    fn get_payload(
        &self,
        location: &Path,
        options: &GetOptions,
        stream: FuturesBytesStream,
    ) -> GetResultPayload {
        let store = self.clone();
        let location = location.clone();
        let options = options.clone();
        let stream = stream.into_send().or_else(move |err| {
            let store = store.clone();
            let location = location.clone();
            let options = options.clone();
            async move {
                // Positioned readers can check the conditions on the first poll.
                let err = match err.downcast::<opendal::Error>() {
                    Ok(err) if err.kind() == ErrorKind::ConditionNotMatch => {
                        return Err(store.format_get_error(&location, &options, err).await);
                    }
                    Ok(err) => io::Error::from(err),
                    Err(err) => err,
                };
                Err(object_store::Error::Generic {
                    store: "IoError",
                    source: Box::new(err),
                })
            }
        });
        GetResultPayload::Stream(Box::pin(stream))
    }

    async fn get_opts_without_stat(
        &self,
        location: &Path,
        raw_location: &str,
        options: &GetOptions,
    ) -> opendal::Result<GetResult> {
        let reader = self
            .inner
            .reader_options(raw_location, format_reader_options(options, None))
            .into_send()
            .await?;

        let mut stream = match options.range.as_ref() {
            Some(GetRange::Bounded(range)) => {
                reader
                    .into_bytes_stream(range.start..range.end)
                    .into_send()
                    .await
            }
            Some(GetRange::Offset(offset)) => reader.into_bytes_stream(*offset..).into_send().await,
            Some(GetRange::Suffix(suffix)) => {
                reader
                    .into_bytes_stream(BytesRange::suffix(*suffix))
                    .into_send()
                    .await
            }
            None => reader.into_bytes_stream(..).into_send().await,
        }?;

        let metadata = stream.metadata().into_send().await?;
        let attributes = format_object_attributes(&metadata);
        let meta = format_object_meta(location.as_ref(), &metadata);
        let read_range = format_read_range(options.range.as_ref(), meta.size)?;

        if read_range.start >= read_range.end {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: read_range,
                meta,
                attributes,
                extensions: Extensions::new(),
            });
        }

        if matches!(
            options.range.as_ref(),
            Some(GetRange::Bounded(range)) if range.end > meta.size
        ) {
            let reader = self
                .inner
                .reader_options(
                    raw_location,
                    format_reader_options(options, Some(meta.size)),
                )
                .into_send()
                .await?;
            stream = reader
                .into_bytes_stream(read_range.start..read_range.end)
                .into_send()
                .await?;
        }

        Ok(GetResult {
            payload: self.get_payload(location, options, stream),
            range: read_range,
            meta,
            attributes,
            extensions: Extensions::new(),
        })
    }

    async fn get_opts_with_stat(
        &self,
        location: &Path,
        raw_location: &str,
        options: &GetOptions,
    ) -> object_store::Result<GetResult> {
        let capability = self.info.capability();
        // Some services can enforce date conditions on read but do not return
        // metadata with that read or accept these conditions on stat.
        let check_locally = !options.head
            && ((options.if_modified_since.is_some() && !capability.stat_with_if_modified_since)
                || (options.if_unmodified_since.is_some()
                    && !capability.stat_with_if_unmodified_since))
            && (options.if_modified_since.is_none() || capability.read_with_if_modified_since)
            && (options.if_unmodified_since.is_none() || capability.read_with_if_unmodified_since)
            && (options.if_match.is_none() || capability.read_with_if_match)
            && (options.if_none_match.is_none() || capability.read_with_if_none_match);
        let stat_options = if check_locally {
            StatOptions {
                version: options.version.clone(),
                ..Default::default()
            }
        } else {
            format_stat_options(options)
        };
        let metadata = self
            .inner
            .stat_options(raw_location, stat_options)
            .into_send()
            .or_else(|err| async { Err(self.format_get_error(location, options, err).await) })
            .await?;
        let attributes = format_object_attributes(&metadata);
        let meta = format_object_meta(location.as_ref(), &metadata);
        if check_locally {
            options.check_preconditions(&meta)?;
        }

        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: 0..0,
                meta,
                attributes,
                extensions: Extensions::new(),
            });
        }

        let read_range = format_read_range(options.range.as_ref(), meta.size)
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        if read_range.start >= read_range.end {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: read_range,
                meta,
                attributes,
                extensions: Extensions::new(),
            });
        }

        let reader = self
            .inner
            .reader_options(
                raw_location,
                format_reader_options(options, Some(meta.size)),
            )
            .into_send()
            .or_else(|err| async { Err(self.format_get_error(location, options, err).await) })
            .await?;
        let stream = reader
            .into_bytes_stream(read_range.start..read_range.end)
            .into_send()
            .or_else(|err| async { Err(self.format_get_error(location, options, err).await) })
            .await?;

        Ok(GetResult {
            payload: self.get_payload(location, options, stream),
            range: read_range,
            meta,
            attributes,
            extensions: Extensions::new(),
        })
    }

    /// Copy a file from one location to another
    async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> object_store::Result<()> {
        let mut copy_options = CopyOptions::default();
        if if_not_exists {
            copy_options.if_not_exists = true;
        }

        // Perform the copy operation
        self.inner
            .copy_options(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
                copy_options,
            )
            .into_send()
            .await
            .map_err(|err| {
                if if_not_exists
                    && matches!(
                        err.kind(),
                        opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch
                    )
                {
                    object_store::Error::AlreadyExists {
                        path: to.to_string(),
                        source: Box::new(err),
                    }
                } else {
                    format_object_store_error(err, from.as_ref())
                }
            })?;

        Ok(())
    }
}

impl Debug for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalStore")
            .field("scheme", &self.info.scheme())
            .field("name", &self.info.name())
            .field("root", &self.info.root())
            .field("capability", &self.info.capability())
            .field("get_ranges_gap", &self.get_ranges_gap)
            .finish()
    }
}

impl Display for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let info = self.inner.info();
        write!(
            f,
            "Opendal({}, bucket={}, root={})",
            info.scheme(),
            info.name(),
            info.root()
        )
    }
}

impl From<Operator> for OpendalStore {
    fn from(value: Operator) -> Self {
        Self::new(value)
    }
}

#[async_trait]
impl ObjectStore for OpendalStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let decoded_location = percent_decode_path(location.as_ref());
        let mut future_write = self
            .inner
            .write_with(&decoded_location, Buffer::from_iter(bytes));
        let opts_mode = opts.mode.clone();
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                future_write = future_write.if_not_exists(true);
            }
            PutMode::Update(update_version) => {
                let Some(etag) = update_version.e_tag else {
                    Err(object_store::Error::NotSupported {
                        source: Box::new(opendal::Error::new(
                            opendal::ErrorKind::Unsupported,
                            "etag is required for conditional put",
                        )),
                    })?
                };
                future_write = future_write.if_match(etag.as_str());
            }
        }
        let mut user_metadata = Vec::new();
        for (attribute, value) in opts.attributes.iter() {
            future_write = match attribute {
                Attribute::CacheControl => future_write.cache_control(value.as_ref()),
                Attribute::ContentDisposition => future_write.content_disposition(value.as_ref()),
                Attribute::ContentEncoding => future_write.content_encoding(value.as_ref()),
                Attribute::ContentLanguage => future_write.content_language(value.as_ref()),
                Attribute::ContentType => future_write.content_type(value.as_ref()),
                Attribute::Metadata(key) => {
                    user_metadata.push((key.to_string(), value.to_string()));
                    future_write
                }
                _ => future_write,
            };
        }
        if !user_metadata.is_empty() {
            future_write = future_write.user_metadata(user_metadata);
        }
        let rp = future_write.into_send().await.map_err(|err| {
            match format_object_store_error(err, location.as_ref()) {
                object_store::Error::Precondition { path, source }
                    if opts_mode == PutMode::Create =>
                {
                    object_store::Error::AlreadyExists { path, source }
                }
                e => e,
            }
        })?;

        let e_tag = rp.etag().map(|s| s.to_string());
        let version = rp.version().map(|s| s.to_string());

        Ok(PutResult {
            e_tag,
            version,
            extensions: Extensions::new(),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let mut options = opendal::options::WriteOptions {
            concurrent: self.multipart_concurrency,
            chunk: self.multipart_chunk_size,
            ..Default::default()
        };

        // Collect user metadata separately to handle multiple entries
        let mut user_metadata = HashMap::new();

        // Handle attributes if provided
        for (key, value) in opts.attributes.iter() {
            match key {
                Attribute::CacheControl => {
                    options.cache_control = Some(value.to_string());
                }
                Attribute::ContentDisposition => {
                    options.content_disposition = Some(value.to_string());
                }
                Attribute::ContentEncoding => {
                    options.content_encoding = Some(value.to_string());
                }
                Attribute::ContentLanguage => {
                    options.content_language = Some(value.to_string());
                }
                Attribute::ContentType => {
                    options.content_type = Some(value.to_string());
                }
                Attribute::Metadata(k) => {
                    user_metadata.insert(k.to_string(), value.to_string());
                }
                _ => {}
            }
        }

        // Apply user metadata if any entries were collected
        if !user_metadata.is_empty() {
            options.user_metadata = Some(user_metadata);
        }

        let decoded_location = percent_decode_path(location.as_ref());
        let writer = self
            .inner
            .writer_options(&decoded_location, options)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let upload = OpendalMultipartUpload::new(writer, location.clone());

        Ok(Box::new(upload))
    }

    async fn get_opts(
        &self,
        location: &Path,
        mut options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let raw_location = percent_decode_path(location.as_ref());
        // Match GetOptions::check_preconditions when forwarding combined conditions.
        if options.if_match.is_some() {
            options.if_unmodified_since = None;
        }
        if options.if_none_match.is_some() {
            options.if_modified_since = None;
        }

        if options.head {
            return self
                .get_opts_with_stat(location, &raw_location, &options)
                .await;
        }

        match self
            .get_opts_without_stat(location, &raw_location, &options)
            .await
        {
            Ok(result) => return Ok(result),
            // Read-open can omit the metadata needed for a GetResult or reject
            // a range beyond EOF. In both cases, use stat to resolve the range.
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::Unsupported | ErrorKind::RangeNotSatisfied
                ) => {}
            Err(err) => return Err(self.format_get_error(location, &options, err).await),
        }

        self.get_opts_with_stat(location, &raw_location, &options)
            .await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        for range in ranges {
            GetRange::Bounded(range.clone()).is_valid().map_err(|err| {
                format_object_store_error(
                    opendal::Error::new(ErrorKind::RangeNotSatisfied, err.to_string()),
                    location.as_ref(),
                )
            })?;
        }

        let raw_location = percent_decode_path(location.as_ref());
        let reader = self
            .inner
            .reader_with(&raw_location)
            .concurrent(DEFAULT_CONCURRENT)
            .gap(self.get_ranges_gap)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        // Avoid a stat for normal ranges. An over-EOF fetch can fail either
        // range validation or OpenDAL's short-read check, so normalize and
        // retry only when the object size changes the requested ranges.
        let buffers = match reader.fetch(ranges.to_vec()).into_send().await {
            Ok(buffers) => buffers,
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::RangeNotSatisfied | ErrorKind::Unexpected
                ) =>
            {
                let metadata = self
                    .inner
                    .stat(&raw_location)
                    .into_send()
                    .await
                    .map_err(|err| format_object_store_error(err, location.as_ref()))?;
                let normalized_ranges = ranges
                    .iter()
                    .map(|range| {
                        format_read_range(
                            Some(&GetRange::Bounded(range.clone())),
                            metadata.content_length(),
                        )
                    })
                    .collect::<opendal::Result<Vec<_>>>()
                    .map_err(|err| format_object_store_error(err, location.as_ref()))?;
                if normalized_ranges.as_slice() == ranges {
                    return Err(format_object_store_error(err, location.as_ref()));
                }
                reader
                    .fetch(normalized_ranges)
                    .into_send()
                    .await
                    .map_err(|err| format_object_store_error(err, location.as_ref()))?
            }
            Err(err) => return Err(format_object_store_error(err, location.as_ref())),
        };

        Ok(buffers.into_iter().map(|buf| buf.to_bytes()).collect())
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let this = self.clone();
        locations
            .and_then(move |location| {
                let this = this.clone();
                async move {
                    let decoded = percent_decode_path(location.as_ref());
                    this.inner
                        .delete(&decoded)
                        .into_send()
                        .await
                        .map_err(|err| format_object_store_error(err, location.as_ref()))?;
                    Ok(location)
                }
                .into_send()
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // object_store `Path` always removes trailing slash
        // need to add it back
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });

        let this = self.clone();
        let fut = async move {
            let stream = this
                .inner
                .lister_with(&path)
                .recursive(true)
                .await
                .map_err(|err| format_object_store_error(err, &path))?;

            // object_store lists objects only, while OpenDAL also returns directories.
            let stream = stream
                .try_filter(|entry| futures::future::ready(!entry.metadata().is_dir()))
                .then(|res| async {
                    let entry = res.map_err(|err| format_object_store_error(err, ""))?;
                    let meta = entry.metadata();

                    Ok(format_object_meta(entry.path(), meta))
                });
            Ok::<_, object_store::Error>(stream)
        };

        fut.into_stream().try_flatten().into_send().boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let offset = offset.clone();

        let this = self.clone();

        let fut = async move {
            // A native cursor can skip keys whose encoded object_store path
            // sorts after the offset. For example, a#b < a$b < a%23b.
            let stream = this
                .inner
                .lister_with(&path)
                .recursive(true)
                .await
                .map_err(|err| format_object_store_error(err, &path))?
                .map_err(move |err| format_object_store_error(err, &path))
                .map_ok(|entry| {
                    let (path, metadata) = entry.into_parts();
                    (Path::from(path.as_str()), path, metadata)
                })
                .try_filter(move |(location, _, metadata)| {
                    futures::future::ready(!metadata.is_dir() && *location > offset)
                })
                .and_then(move |(location, path, metadata)| {
                    let this = this.clone();
                    async move {
                        let metadata = if metadata.last_modified().is_some() {
                            metadata
                        } else {
                            this.inner
                                .stat(&path)
                                .await
                                .map_err(|err| format_object_store_error(err, &path))?
                        };
                        Ok(format_object_meta(location, &metadata))
                    }
                });

            Ok::<_, object_store::Error>(stream)
        };

        fut.into_stream().try_flatten().into_send().boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let mut stream = self
            .inner
            .lister_with(&path)
            .into_future()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, &path))?
            .into_send();

        // OpenDAL also returns the listed path itself when it exists, such as a
        // directory on fs or a directory marker on S3. object_store never
        // reports a prefix as its own common prefix.
        let listed_path = normalize_path(&path);

        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();

        while let Some(res) = stream.next().into_send().await {
            let entry = res.map_err(|err| format_object_store_error(err, ""))?;
            let meta = entry.metadata();

            if meta.is_dir() {
                if entry.path() != listed_path {
                    common_prefixes.push(entry.path().into());
                }
            } else if meta.last_modified().is_some() {
                objects.push(format_object_meta(entry.path(), meta));
            } else {
                let meta = self
                    .inner
                    .stat(entry.path())
                    .into_send()
                    .await
                    .map_err(|err| format_object_store_error(err, entry.path()))?;
                objects.push(format_object_meta(entry.path(), &meta));
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
            extensions: Extensions::new(),
        })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: ObjectStoreCopyOptions,
    ) -> object_store::Result<()> {
        let if_not_exists = matches!(options.mode, ObjectStoreCopyMode::Create);
        self.copy_request(from, to, if_not_exists).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let capability = self.info().capability();
        let (supports_rename, copy_mode) = match options.target_mode {
            RenameTargetMode::Overwrite => (capability.rename, ObjectStoreCopyMode::Overwrite),
            RenameTargetMode::Create => (
                capability.rename_with_if_not_exists,
                ObjectStoreCopyMode::Create,
            ),
        };
        if !supports_rename {
            self.copy_opts(
                from,
                to,
                ObjectStoreCopyOptions {
                    mode: copy_mode,
                    extensions: options.extensions,
                },
            )
            .await?;
            return self.delete(from).await;
        }

        let if_not_exists = options.target_mode == RenameTargetMode::Create;
        self.inner
            .rename_with(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
            )
            .if_not_exists(if_not_exists)
            .into_send()
            .await
            .map_err(|err| {
                if if_not_exists
                    && matches!(
                        err.kind(),
                        opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch
                    )
                {
                    object_store::Error::AlreadyExists {
                        path: to.to_string(),
                        source: Box::new(err),
                    }
                } else {
                    format_object_store_error(err, from.as_ref())
                }
            })
    }
}

/// `MultipartUpload`'s impl based on `Writer` in opendal
///
/// # Notes
///
/// OpenDAL writer can handle concurrent internally we don't generate real `UploadPart` like existing
/// implementation do. Instead, we just write the part and notify the next task to be written.
///
/// The lock here doesn't really involve the write process, it's just for the notify mechanism.
struct OpendalMultipartUpload {
    writer: Arc<Mutex<Writer>>,
    location: Path,
    next_notify: oneshot::Receiver<()>,
}

impl OpendalMultipartUpload {
    fn new(writer: Writer, location: Path) -> Self {
        // an immediately dropped sender for the first part to write without waiting
        let (_, rx) = oneshot::channel();

        Self {
            writer: Arc::new(Mutex::new(writer)),
            location,
            next_notify: rx,
        }
    }
}

#[async_trait]
impl MultipartUpload for OpendalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = self.writer.clone();
        let location = self.location.clone();

        // Generate next notify which will be notified after the current part is written.
        let (tx, rx) = oneshot::channel();
        // Fetch the notify for current part to wait for it to be written.
        let last_rx = std::mem::replace(&mut self.next_notify, rx);

        async move {
            // Wait for the previous part to be written
            let _ = last_rx.await;

            let mut writer = writer.lock().await;
            let result = writer
                .write(Buffer::from_iter(data))
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()));

            // Notify the next part to be written
            drop(tx);

            result
        }
        .into_send()
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut writer = self.writer.lock().await;
        let metadata = writer
            .close()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))?;

        let e_tag = metadata.etag().map(|s| s.to_string());
        let version = metadata.version().map(|s| s.to_string());

        Ok(PutResult {
            e_tag,
            version,
            extensions: Extensions::new(),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .abort()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))
    }
}

impl Debug for OpendalMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalMultipartUpload")
            .field("location", &self.location)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};
    use opendal::services;
    use rand::prelude::*;
    use rand::rng;
    use std::sync::Arc;

    use super::*;

    async fn create_test_object_store() -> Arc<dyn ObjectStore> {
        let op = Operator::new(services::Memory::default()).unwrap();
        let object_store = Arc::new(OpendalStore::new(op));

        let path: Path = "data/test.txt".into();
        let bytes = Bytes::from_static(b"hello, world!");
        object_store.put(&path, bytes.into()).await.unwrap();

        let path: Path = "data/nested/test.txt".into();
        let bytes = Bytes::from_static(b"hello, world! I am nested.");
        object_store.put(&path, bytes.into()).await.unwrap();

        object_store
    }

    #[tokio::test]
    async fn test_basic() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(op));

        // Retrieve a specific file
        let path: Path = "data/test.txt".into();

        let bytes = Bytes::from_static(b"hello, world!");
        object_store.put(&path, bytes.clone().into()).await.unwrap();

        let meta = object_store.head(&path).await.unwrap();

        assert_eq!(meta.size, 13);

        assert_eq!(
            object_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes
        );
    }

    async fn assert_get_opts_range_contract(
        store: &OpendalStore,
        reference: &object_store::memory::InMemory,
        location: &Path,
        range: Option<GetRange>,
        expected: Option<(Range<u64>, &'static [u8])>,
    ) {
        async fn get_opts_range_and_bytes(
            store: &dyn ObjectStore,
            location: &Path,
            range: Option<GetRange>,
        ) -> object_store::Result<(Range<u64>, Bytes)> {
            let result = store
                .get_opts(
                    location,
                    GetOptions {
                        range,
                        ..Default::default()
                    },
                )
                .await?;
            let range = result.range.clone();
            let bytes = result.bytes().await?;
            Ok((range, bytes))
        }

        let actual = get_opts_range_and_bytes(store, location, range.clone()).await;
        let actual_from_reference =
            get_opts_range_and_bytes(reference, location, range.clone()).await;
        match expected {
            Some((expected_range, expected_bytes)) => {
                let actual = actual.unwrap();
                assert_eq!(actual.0, expected_range, "range {range:?}");
                assert_eq!(actual.1, expected_bytes, "range {range:?}");
                assert_eq!(actual, actual_from_reference.unwrap(), "range {range:?}");
            }
            None => {
                assert!(actual.is_err(), "range {range:?} must be rejected");
                assert!(
                    actual_from_reference.is_err(),
                    "InMemory must reject range {range:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_get_range_and_get_ranges_contract() {
        let store = OpendalStore::new(Operator::new(services::Memory::default()).unwrap());
        let location = Path::from("range-contract");
        store
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();

        for range in [0..0, 1..1, 3..4, 4..5, Range { start: 2, end: 1 }] {
            assert!(
                store.get_range(&location, range.clone()).await.is_err(),
                "range {range:?} must be rejected"
            );
        }
        assert_eq!(
            store.get_range(&location, 0..9).await.unwrap(),
            Bytes::from_static(b"abc")
        );
        assert_eq!(
            store.get_ranges(&location, &[0..9, 1..9]).await.unwrap(),
            vec![Bytes::from_static(b"abc"), Bytes::from_static(b"bc")]
        );
        for range in [1..1, 5..6] {
            let ranges = std::slice::from_ref(&range);
            assert!(
                store.get_ranges(&location, ranges).await.is_err(),
                "range {range:?} must be rejected"
            );
        }

        let empty_location = Path::from("empty-range-contract");
        store
            .put(&empty_location, Bytes::new().into())
            .await
            .unwrap();
        for range in [0..0, 0..1] {
            assert!(
                store
                    .get_range(&empty_location, range.clone())
                    .await
                    .is_err(),
                "empty object range {range:?} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn test_get_opts_range_contract_matches_in_memory() {
        let store = OpendalStore::new(Operator::new(services::Memory::default()).unwrap());
        let reference = object_store::memory::InMemory::new();
        let location = Path::from("range-contract");
        store
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        reference
            .put(&location, Bytes::from_static(b"abc").into())
            .await
            .unwrap();

        let cases = [
            (
                Some(GetRange::Bounded(0..9)),
                Some((0..3, b"abc".as_slice())),
            ),
            (Some(GetRange::Offset(2)), Some((2..3, b"c".as_slice()))),
            (Some(GetRange::Offset(3)), None),
            (Some(GetRange::Offset(4)), None),
            (Some(GetRange::Suffix(0)), Some((3..3, b"".as_slice()))),
            (Some(GetRange::Suffix(4)), Some((0..3, b"abc".as_slice()))),
            (Some(GetRange::Bounded(1..1)), None),
            (Some(GetRange::Bounded(Range { start: 2, end: 1 })), None),
            (Some(GetRange::Bounded(3..4)), None),
            (Some(GetRange::Bounded(4..5)), None),
        ];
        for (range, expected) in cases {
            assert_get_opts_range_contract(&store, &reference, &location, range, expected).await;
        }

        let empty_location = Path::from("empty-range-contract");
        store
            .put(&empty_location, Bytes::new().into())
            .await
            .unwrap();
        reference
            .put(&empty_location, Bytes::new().into())
            .await
            .unwrap();
        let empty_cases = [
            (None, Some((0..0, b"".as_slice()))),
            (Some(GetRange::Bounded(0..0)), None),
            (Some(GetRange::Bounded(0..1)), None),
            (Some(GetRange::Offset(0)), None),
            (Some(GetRange::Offset(1)), None),
            (Some(GetRange::Suffix(0)), Some((0..0, b"".as_slice()))),
            (Some(GetRange::Suffix(1)), Some((0..0, b"".as_slice()))),
        ];
        for (range, expected) in empty_cases {
            assert_get_opts_range_contract(&store, &reference, &empty_location, range, expected)
                .await;
        }
    }

    #[tokio::test]
    async fn test_content_attributes_round_trip() -> object_store::Result<()> {
        let op = Operator::new(services::Memory::default()).unwrap();
        let store = OpendalStore::new(op);
        let attributes = Attributes::from_iter([
            (Attribute::CacheControl, "max-age=300"),
            (
                Attribute::ContentDisposition,
                "attachment; filename=hello.txt",
            ),
            (Attribute::ContentEncoding, "identity"),
            (Attribute::ContentLanguage, "fr-CA, en"),
            (Attribute::ContentType, "text/plain"),
        ]);
        let single = Path::from("single.txt");
        let multipart = Path::from("multipart.txt");
        let body = Bytes::from_static(b"hello");

        store
            .put_opts(
                &single,
                body.clone().into(),
                PutOptions {
                    attributes: attributes.clone(),
                    ..Default::default()
                },
            )
            .await?;
        let mut upload = store
            .put_multipart_opts(
                &multipart,
                PutMultipartOptions {
                    attributes: attributes.clone(),
                    ..Default::default()
                },
            )
            .await?;
        upload.put_part(body.slice(..2).into()).await?;
        upload.put_part(body.slice(2..).into()).await?;
        upload.complete().await?;

        for path in [single, multipart] {
            let head = store
                .get_opts(&path, GetOptions::new().with_head(true))
                .await?;
            assert_eq!(head.attributes, attributes);
            assert_eq!(head.meta.location, path);
            assert_eq!(head.meta.size, body.len() as u64);
            assert_eq!(store.head(&path).await?, head.meta);

            let get = store.get(&path).await?;
            assert_eq!(get.attributes, attributes);
            assert_eq!(get.meta, head.meta);
            assert_eq!(get.bytes().await?, body);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_fs_get_rechecks_date_when_reading_payload() -> anyhow::Result<()> {
        let root =
            std::env::temp_dir().join(format!("opendal-object-store-{}", uuid::Uuid::new_v4()));
        let store = OpendalStore::new(Operator::new(
            services::Fs::default().root(root.to_str().unwrap()),
        )?);
        let location = Path::from("object");
        let result = async {
            store
                .put(&location, Bytes::from_static(b"data").into())
                .await?;
            let meta = store.head(&location).await?;
            let result = store
                .get_opts(
                    &location,
                    GetOptions {
                        if_modified_since: Some(
                            meta.last_modified - std::time::Duration::from_secs(1),
                        ),
                        ..Default::default()
                    },
                )
                .await?;
            let file = std::fs::File::options()
                .write(true)
                .open(root.join("object"))?;
            file.set_modified(std::time::SystemTime::UNIX_EPOCH)?;
            Ok::<_, anyhow::Error>(result.bytes().await)
        }
        .await;
        tokio::fs::remove_dir_all(root).await?;
        assert!(matches!(
            result?,
            Err(object_store::Error::NotModified { path, .. }) if path == location.as_ref()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_rename_overwrite_fs() -> anyhow::Result<()> {
        let root =
            std::env::temp_dir().join(format!("opendal-object-store-{}", uuid::Uuid::new_v4()));
        let op = Operator::new(services::Fs::default().root(root.to_str().unwrap()))?;
        let store = OpendalStore::new(op);
        let from = Path::from("source");
        let to = Path::from("target");
        let nested = Path::from("new/parent/a~b");
        let source = Bytes::from_static(b"source contents");
        store.put(&from, source.clone().into()).await?;
        store
            .put(&to, Bytes::from_static(b"old target").into())
            .await?;
        #[cfg(unix)]
        let source_inode = {
            use std::os::unix::fs::MetadataExt;
            tokio::fs::metadata(root.join("source")).await?.ino()
        };

        store.rename(&from, &to).await?;
        assert_eq!(store.get(&to).await?.bytes().await?, source);
        assert!(matches!(
            store.head(&from).await,
            Err(object_store::Error::NotFound { .. })
        ));
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let target_inode = tokio::fs::metadata(root.join("target")).await?.ino();
            assert_eq!(target_inode, source_inode);
        }

        let err = store.rename(&from, &to).await.unwrap_err();
        assert!(matches!(err, object_store::Error::NotFound { path, .. } if path == from.as_ref()));
        assert_eq!(store.get(&to).await?.bytes().await?, source);

        store.rename(&to, &nested).await?;
        assert_eq!(tokio::fs::read(root.join("new/parent/a~b")).await?, source);
        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_rename_create_fs() -> anyhow::Result<()> {
        let root =
            std::env::temp_dir().join(format!("opendal-object-store-{}", uuid::Uuid::new_v4()));
        let op = Operator::new(services::Fs::default().root(root.to_str().unwrap()))?;
        let store = OpendalStore::new(op);
        let from = Path::from("source");
        let to = Path::from("target");
        let absent = Path::from("new/parent/target");
        let source = Bytes::from_static(b"source contents");
        let target = Bytes::from_static(b"target contents");
        store.put(&from, source.clone().into()).await?;
        store.put(&to, target.clone().into()).await?;

        let err = store.rename_if_not_exists(&from, &to).await.unwrap_err();
        let native_create = store.info().capability().rename_with_if_not_exists;
        if native_create {
            assert!(
                matches!(err, object_store::Error::AlreadyExists { path, .. } if path == to.as_ref())
            );
        } else {
            assert!(matches!(err, object_store::Error::NotSupported { .. }));
        }
        assert_eq!(store.get(&from).await?.bytes().await?, source);
        assert_eq!(store.get(&to).await?.bytes().await?, target);

        let result = store.rename_if_not_exists(&from, &absent).await;
        if native_create {
            result?;
            assert_eq!(store.get(&absent).await?.bytes().await?, source);
            assert!(matches!(
                store.head(&from).await,
                Err(object_store::Error::NotFound { .. })
            ));
        } else {
            assert!(matches!(
                result,
                Err(object_store::Error::NotSupported { .. })
            ));
            assert_eq!(store.get(&from).await?.bytes().await?, source);
            assert!(matches!(
                store.head(&absent).await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
        tokio::fs::remove_dir_all(root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_put_multipart() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(op));

        let mut rng = rng();

        // Case complete
        let path: Path = "data/test_complete.txt".into();
        let upload = object_store.put_multipart(&path).await.unwrap();

        let mut write = WriteMultipart::new(upload);

        let mut all_bytes = vec![];
        let round = rng.random_range(1..=1024);
        for _ in 0..round {
            let size = rng.random_range(1..=1024);
            let mut bytes = vec![0; size];
            rng.fill_bytes(&mut bytes);

            all_bytes.extend_from_slice(&bytes);
            write.put(bytes.into());
        }

        let _ = write.finish().await.unwrap();

        let meta = object_store.head(&path).await.unwrap();

        assert_eq!(meta.size, all_bytes.len() as u64);

        assert_eq!(
            object_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from(all_bytes)
        );

        // Case abort
        let path: Path = "data/test_abort.txt".into();
        let mut upload = object_store.put_multipart(&path).await.unwrap();
        upload.put_part(vec![1; 1024].into()).await.unwrap();
        upload.abort().await.unwrap();

        let res = object_store.head(&path).await;
        let err = res.unwrap_err();

        assert!(matches!(err, object_store::Error::NotFound { .. }))
    }

    #[tokio::test]
    async fn test_list() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let results = object_store.list(Some(&path)).collect::<Vec<_>>().await;
        assert_eq!(results.len(), 2);
        let mut locations = results
            .iter()
            .map(|x| x.as_ref().unwrap().location.as_ref())
            .collect::<Vec<_>>();

        let expected_files = vec![
            (
                "data/nested/test.txt",
                Bytes::from_static(b"hello, world! I am nested."),
            ),
            ("data/test.txt", Bytes::from_static(b"hello, world!")),
        ];

        let expected_locations = expected_files.iter().map(|x| x.0).collect::<Vec<&str>>();

        locations.sort();
        assert_eq!(locations, expected_locations);

        for (location, bytes) in expected_files {
            let path: Path = location.into();
            assert_eq!(
                object_store
                    .get(&path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                bytes
            );
        }
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let result = object_store.list_with_delimiter(Some(&path)).await.unwrap();
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.common_prefixes.len(), 1);
        assert_eq!(result.objects[0].location.as_ref(), "data/test.txt");
        assert_eq!(result.common_prefixes[0].as_ref(), "data/nested");
    }

    #[tokio::test]
    async fn test_list_with_delimiter_skips_listed_prefix() {
        let op = Operator::new(services::Memory::default()).unwrap();
        // Make `data/` exist as a directory, so OpenDAL returns it when it is listed.
        op.create_dir("data/").await.unwrap();
        let object_store = OpendalStore::new(op);

        let bytes = Bytes::from_static(b"hello, world!");
        object_store
            .put(&"data/test.txt".into(), bytes.clone().into())
            .await
            .unwrap();
        object_store
            .put(&"data/nested/test.txt".into(), bytes.into())
            .await
            .unwrap();

        let path: Path = "data".into();
        let result = object_store.list_with_delimiter(Some(&path)).await.unwrap();
        assert_eq!(result.common_prefixes, vec![Path::from("data/nested")]);
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].location.as_ref(), "data/test.txt");
    }

    #[tokio::test]
    async fn test_list_skips_directories() {
        let op = Operator::new(services::Memory::default()).unwrap();
        op.create_dir("data/").await.unwrap();
        op.create_dir("data/nested/").await.unwrap();
        let object_store = OpendalStore::new(op);

        let bytes = Bytes::from_static(b"hello, world!");
        object_store
            .put(&"data/test.txt".into(), bytes.clone().into())
            .await
            .unwrap();
        object_store
            .put(&"data/nested/test.txt".into(), bytes.into())
            .await
            .unwrap();

        let path: Path = "data".into();
        let mut locations = object_store
            .list(Some(&path))
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        locations.sort();
        assert_eq!(locations, vec!["data/nested/test.txt", "data/test.txt"]);

        let offset: Path = "data/nested/test.txt".into();
        let locations = object_store
            .list_with_offset(Some(&path), &offset)
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(locations, vec!["data/test.txt"]);
    }

    #[tokio::test]
    async fn test_list_with_offset() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let offset: Path = "data/nested/test.txt".into();
        let result = object_store
            .list_with_offset(Some(&path), &offset)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].as_ref().unwrap().location.as_ref(),
            "data/test.txt"
        );
    }

    #[tokio::test]
    async fn test_list_with_offset_fs_uses_encoded_path_order() {
        let root =
            std::env::temp_dir().join(format!("opendal-object-store-{}", uuid::Uuid::new_v4()));
        let op = Operator::new(services::Fs::default().root(root.to_str().unwrap())).unwrap();
        let store = OpendalStore::new(op);
        for path in ["a~b", "a-b", "z"] {
            store
                .put(&Path::from(path), Bytes::from_static(b"data").into())
                .await
                .unwrap();
        }

        let offset = Path::from("a~b");
        let result = store
            .list_with_offset(None, &offset)
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<_>>()
            .await;
        tokio::fs::remove_dir_all(root).await.unwrap();

        let mut locations = result.unwrap();
        locations.sort();
        assert_eq!(locations, vec![Path::from("a-b"), Path::from("z")]);
    }

    #[tokio::test]
    async fn test_list_with_offset_memory_uses_encoded_path_order() {
        let store = OpendalStore::new(Operator::new(services::Memory::default()).unwrap());
        for path in ["a#b", "a$b", "a-b", "a~b"] {
            store
                .put(&Path::from(path), Bytes::from_static(b"data").into())
                .await
                .unwrap();
        }

        for (offset, expected) in [("a$b", vec!["a#b", "a~b", "a-b"]), ("a~b", vec!["a-b"])] {
            let mut locations: Vec<_> = store
                .list_with_offset(None, &Path::from(offset))
                .map_ok(|meta| meta.location)
                .try_collect()
                .await
                .unwrap();
            locations.sort_unstable();
            assert_eq!(
                locations,
                expected.into_iter().map(Path::from).collect::<Vec<_>>()
            );
        }
    }

    /// Custom layer that counts stat operations and optionally records read ranges.
    mod stat_counter {
        use super::*;
        use std::sync::Mutex as StdMutex;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug, Clone)]
        pub struct StatCounterLayer {
            count: Arc<AtomicUsize>,
            read_ranges: Option<Arc<StdMutex<Vec<BytesRange>>>>,
        }

        impl StatCounterLayer {
            pub fn new(count: Arc<AtomicUsize>) -> Self {
                Self {
                    count,
                    read_ranges: None,
                }
            }

            pub fn with_read_ranges(mut self, read_ranges: Arc<StdMutex<Vec<BytesRange>>>) -> Self {
                self.read_ranges = Some(read_ranges);
                self
            }
        }

        impl opendal::raw::Layer for StatCounterLayer {
            fn apply_service(&self, srv: opendal::raw::Servicer) -> opendal::raw::Servicer {
                Arc::new(StatCounterService {
                    srv,
                    count: self.count.clone(),
                    read_ranges: self.read_ranges.clone(),
                })
            }
        }

        #[derive(Debug)]
        pub struct StatCounterService {
            srv: opendal::raw::Servicer,
            count: Arc<AtomicUsize>,
            read_ranges: Option<Arc<StdMutex<Vec<BytesRange>>>>,
        }

        struct ReadCounter {
            inner: opendal::raw::oio::Reader,
            ranges: Arc<StdMutex<Vec<BytesRange>>>,
        }

        impl opendal::raw::oio::Read for ReadCounter {
            async fn open(
                &self,
                range: BytesRange,
            ) -> opendal::Result<(
                opendal::raw::RpRead,
                Box<dyn opendal::raw::oio::ReadStreamDyn>,
            )> {
                self.inner.open(range).await
            }

            async fn read(
                &self,
                range: BytesRange,
            ) -> opendal::Result<(opendal::raw::RpRead, Buffer)> {
                self.ranges.lock().unwrap().push(range);
                self.inner.read(range).await
            }
        }

        impl opendal::raw::Service for StatCounterService {
            type Reader = opendal::raw::oio::Reader;
            type Writer = opendal::raw::oio::Writer;
            type Lister = opendal::raw::oio::Lister;
            type Deleter = opendal::raw::oio::Deleter;
            type Copier = opendal::raw::oio::Copier;
            type Composer = opendal::raw::oio::Composer;

            fn info(&self) -> opendal::raw::ServiceInfo {
                self.srv.info()
            }

            fn capability(&self) -> opendal::Capability {
                self.srv.capability()
            }

            async fn create_dir(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpCreateDir,
            ) -> opendal::Result<opendal::raw::RpCreateDir> {
                self.srv.create_dir(ctx, path, args).await
            }

            async fn stat(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpStat,
            ) -> opendal::Result<opendal::raw::RpStat> {
                self.count.fetch_add(1, Ordering::SeqCst);
                self.srv.stat(ctx, path, args).await
            }

            fn read(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpRead,
            ) -> opendal::Result<Self::Reader> {
                let reader = self.srv.read(ctx, path, args)?;
                match &self.read_ranges {
                    Some(ranges) => Ok(Box::new(ReadCounter {
                        inner: reader,
                        ranges: ranges.clone(),
                    })),
                    None => Ok(reader),
                }
            }

            fn write(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpWrite,
            ) -> opendal::Result<Self::Writer> {
                self.srv.write(ctx, path, args)
            }

            fn delete(&self, ctx: &opendal::OperationContext) -> opendal::Result<Self::Deleter> {
                self.srv.delete(ctx)
            }

            fn list(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpList,
            ) -> opendal::Result<Self::Lister> {
                self.srv.list(ctx, path, args)
            }

            fn copy(
                &self,
                ctx: &opendal::OperationContext,
                from: &str,
                to: &str,
                args: opendal::raw::OpCopy,
            ) -> opendal::Result<Self::Copier> {
                self.srv.copy(ctx, from, to, args)
            }

            async fn rename(
                &self,
                ctx: &opendal::OperationContext,
                from: &str,
                to: &str,
                args: opendal::raw::OpRename,
            ) -> opendal::Result<opendal::raw::RpRename> {
                self.srv.rename(ctx, from, to, args).await
            }

            async fn presign(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpPresign,
            ) -> opendal::Result<opendal::raw::RpPresign> {
                self.srv.presign(ctx, path, args).await
            }
        }
    }

    #[tokio::test]
    async fn test_get_ranges_gap_configuration() {
        use std::sync::Mutex as StdMutex;
        use std::sync::atomic::AtomicUsize;

        let read_ranges = Arc::new(StdMutex::new(Vec::new()));
        let op = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .layer(
                stat_counter::StatCounterLayer::new(Arc::new(AtomicUsize::new(0)))
                    .with_read_ranges(read_ranges.clone()),
            );
        let store = OpendalStore::new(op);
        let location = "test_get_ranges_fetch.txt".into();
        store
            .put(&location, Bytes::from_static(b"0123456789abcdefgh").into())
            .await
            .unwrap();

        let ranges = [15..17, 0..4, 8..10];
        let result = store.get_ranges(&location, &ranges).await.unwrap();
        assert_eq!(
            result,
            vec![
                Bytes::from_static(b"fg"),
                Bytes::from_static(b"0123"),
                Bytes::from_static(b"89"),
            ]
        );
        assert_eq!(
            read_ranges.lock().unwrap().as_slice(),
            &[BytesRange::new(0, Some(17))]
        );

        read_ranges.lock().unwrap().clear();
        store
            .clone()
            .with_get_ranges_gap(0)
            .get_ranges(&location, &ranges)
            .await
            .unwrap();
        let mut actual = read_ranges.lock().unwrap().clone();
        actual.sort_unstable_by_key(BytesRange::offset);
        assert_eq!(
            actual,
            vec![
                BytesRange::new(0, Some(4)),
                BytesRange::new(8, Some(2)),
                BytesRange::new(15, Some(2)),
            ]
        );

        read_ranges.lock().unwrap().clear();
        store
            .with_get_ranges_gap(4)
            .get_ranges(&location, &ranges)
            .await
            .unwrap();
        let mut actual = read_ranges.lock().unwrap().clone();
        actual.sort_unstable_by_key(BytesRange::offset);
        assert_eq!(
            actual,
            vec![BytesRange::new(0, Some(10)), BytesRange::new(15, Some(2))]
        );
    }

    #[tokio::test]
    async fn test_get_range_no_stat() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Create a stat counter and operator with tracking layer
        let stat_count = Arc::new(AtomicUsize::new(0));
        let op = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .layer(stat_counter::StatCounterLayer::new(stat_count.clone()));
        let store = OpendalStore::new(op);

        // Create a test file
        let location = "test_get_range.txt".into();
        let value = Bytes::from_static(b"Hello, world!");
        store.put(&location, value.clone().into()).await.unwrap();

        // Reset counter after put
        stat_count.store(0, Ordering::SeqCst);

        // Test 1: get_ranges should NOT call stat()
        #[allow(clippy::single_range_in_vec_init)]
        let ranges = [0u64..5];
        let ret = store.get_ranges(&location, &ranges).await.unwrap();
        assert_eq!(Bytes::from_static(b"Hello"), ret[0]);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_ranges should not call stat()"
        );

        // Reset counter
        stat_count.store(0, Ordering::SeqCst);

        // Test 2: get_opts should NOT call stat() for bounded range reads
        let opts = object_store::GetOptions {
            range: Some(object_store::GetRange::Bounded(0..5)),
            ..Default::default()
        };
        let ret = store.get_opts(&location, opts).await.unwrap();
        assert_eq!(ret.meta.size, value.len() as u64);
        assert_eq!(ret.range, 0..5);
        let data = ret.bytes().await.unwrap();
        assert_eq!(Bytes::from_static(b"Hello"), data);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_opts should not call stat() for bounded range reads"
        );

        // Reset counter
        stat_count.store(0, Ordering::SeqCst);

        // Test 3: get_opts should NOT call stat() for offset range reads
        let opts = object_store::GetOptions {
            range: Some(object_store::GetRange::Offset(7)),
            ..Default::default()
        };
        let ret = store.get_opts(&location, opts).await.unwrap();
        assert_eq!(ret.meta.size, value.len() as u64);
        assert_eq!(ret.range, 7..value.len() as u64);
        let data = ret.bytes().await.unwrap();
        assert_eq!(Bytes::from_static(b"world!"), data);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_opts should not call stat() for offset range reads"
        );

        // Reset counter
        stat_count.store(0, Ordering::SeqCst);

        // Test 4: get_opts should NOT call stat() for full reads
        let ret = store
            .get_opts(&location, object_store::GetOptions::default())
            .await
            .unwrap();
        assert_eq!(ret.meta.size, value.len() as u64);
        assert_eq!(ret.range, 0..value.len() as u64);
        let data = ret.bytes().await.unwrap();
        assert_eq!(value, data);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_opts should not call stat() for full reads"
        );

        // Reset counter
        stat_count.store(0, Ordering::SeqCst);

        // Test 5: get_opts should NOT call stat() for suffix range reads
        let opts = object_store::GetOptions {
            range: Some(object_store::GetRange::Suffix(6)),
            ..Default::default()
        };
        let ret = store.get_opts(&location, opts).await.unwrap();
        assert_eq!(ret.meta.size, value.len() as u64);
        assert_eq!(ret.range, 7..value.len() as u64);
        let data = ret.bytes().await.unwrap();
        assert_eq!(Bytes::from_static(b"world!"), data);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_opts should not call stat() for suffix range reads"
        );

        // Cleanup
        store.delete(&location).await.unwrap();
    }
}
