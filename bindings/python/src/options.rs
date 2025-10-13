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

use crate::gen_stub_pyclass;
use dict_derive::FromPyObject;
use jiff::Timestamp;
use opendal::{self as ocore, raw::BytesRange};
use pyo3::pyclass;
use std::collections::HashMap;

/// Options for read operations.
#[gen_stub_pyclass]
#[pyclass(get_all, set_all, module = "opendal.options")]
#[derive(FromPyObject, Default)]

pub struct ReadOptions {
    /// Set the starting point for the read.
    ///
    /// This specifies the byte offset from the beginning of the file to
    /// start reading from. If not set, the read will start from the
    /// beginning (offset 0).
    #[gen_stub(default = 0)]
    pub offset: u64,

    /// Set the number of bytes to read.
    ///
    /// This specifies the total number of bytes to read starting from the
    /// `offset`. If not set, the read will continue to the end of the file.
    #[gen_stub(default = ReadOptions::default().size)]
    pub size: Option<u64>,

    /// Set `version` for this operation.
    ///
    /// This option can be used to retrieve the data of a specified version
    /// of the given path.
    ///
    /// If the version does not exist, an error will be returned.
    #[gen_stub(default = ReadOptions::default().version)]
    pub version: Option<String>,

    /// Set `if_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` matches the
    /// given `ETag`.
    ///
    /// If the file exists and its ETag does not match, an error will be
    /// returned.
    #[gen_stub(default = ReadOptions::default().if_match)]
    pub if_match: Option<String>,

    /// Set `if_none_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` does not match
    /// the given `ETag`.
    ///
    /// If the file exists and its ETag matches, an error will be returned.
    #[gen_stub(default = ReadOptions::default().if_none_match)]
    pub if_none_match: Option<String>,

    /// Set `if_modified_since` for this operation.
    ///
    /// This option can be used to check if the file has been modified since
    /// the given timestamp.
    ///
    /// If the file exists and has not been modified since the specified time,
    /// an error will be returned.
    #[gen_stub(default = ReadOptions::default().if_modified_since)]
    pub if_modified_since: Option<Timestamp>,

    /// Set `if_unmodified_since` for this operation.
    ///
    /// This feature can be used to check if the file has not been modified
    /// since the given timestamp.
    ///
    /// If the file exists and has been modified since the specified time, an
    /// error will be returned.
    #[gen_stub(default = ReadOptions::default().if_unmodified_since)]
    pub if_unmodified_since: Option<Timestamp>,

    /// Set the level of concurrency for the read operation.
    ///
    /// By default, files are read sequentially. Setting this enables
    /// concurrent reads, which can improve performance for large files on
    /// supported services. It determines how many chunks are fetched
    /// at the same time.
    #[gen_stub(default = ReadOptions::default().concurrent)]
    pub concurrent: usize,

    /// Set the chunk size for concurrent reads.
    ///
    /// Services often have a preferred chunk size that is used by default.
    /// You can override this based on your application's needs.
    #[gen_stub(default = ReadOptions::default().chunk)]
    pub chunk: Option<usize>,

    /// Set the gap size for merging range read requests.
    ///
    /// If the gap between two requested byte ranges is smaller than this
    /// value, the ranges will be merged into a single read request. This
    /// reduces API calls at the cost of fetching extra data.
    #[gen_stub(default = ReadOptions::default().gap)]
    pub gap: Option<usize>,

    /// Set the number of chunks to prefetch in memory.
    ///
    /// During concurrent reads, this limits how many completed chunks can
    /// be buffered. Once the limit is reached, new read tasks are paused
    /// until the buffer is consumed. A higher value allows for more
    /// aggressive prefetching at the cost of higher memory usage.
    ///
    /// Defaults to 0 (no prefetching).
    #[gen_stub(default = 0)]
    pub prefetch: usize,

    /// Override the `content-type` header in the response.
    ///
    /// This is only effective for presigned read operations.
    #[gen_stub(default = ReadOptions::default().override_content_type)]
    pub override_content_type: Option<String>,

    /// Override the `cache-control` header in the response.
    ///
    /// This is only effective for presigned read operations.
    #[gen_stub(default = ReadOptions::default().override_cache_control)]
    pub override_cache_control: Option<String>,

    /// Override the `content-disposition` header in the response.
    ///
    /// This is only effective for presigned read operations.
    #[gen_stub(default = ReadOptions::default().override_content_disposition)]
    pub override_content_disposition: Option<String>,
}

impl ReadOptions {
    pub fn make_range(&self) -> BytesRange {
        BytesRange::new(self.offset, self.size)
    }
}

/// Options for write operations.
#[gen_stub_pyclass]
#[pyclass(get_all, set_all, module = "opendal.options")]
#[derive(FromPyObject, Default)]
pub struct WriteOptions {
    /// Enable append mode for this operation.
    ///
    /// If `true`, new data will be appended to the end of the file. If the
    /// file does not exist, it will be created. By default, write operations
    /// overwrite existing files. Check if the service supports this first.
    #[gen_stub(default = WriteOptions::default().append)]
    pub append: bool,

    /// Set the `Cache-Control` header for the object.
    ///
    /// This controls caching behavior for browsers and CDNs. The value should
    /// be a valid HTTP `Cache-Control` header string. It is ignored if the
    /// service does not support it.
    #[gen_stub(default = WriteOptions::default().cache_control)]
    pub cache_control: Option<String>,

    /// Set the `Content-Type` header for the object.
    ///
    /// This specifies the media type (MIME type) of the content, such as
    /// `text/plain` or `image/jpeg`. It is ignored if not supported by the
    /// service.
    #[gen_stub(default = WriteOptions::default().content_type)]
    pub content_type: Option<String>,

    /// Set the `Content-Disposition` header for the object.
    ///
    /// This suggests how content should be handled by a browser, for example,
    /// `inline` (displayed) or `attachment; filename="..."` (downloaded).
    /// It is ignored if unsupported.
    #[gen_stub(default = WriteOptions::default().content_disposition)]
    pub content_disposition: Option<String>,

    /// Set the `Content-Encoding` header for the object.
    ///
    /// This specifies the encoding of the content, such as `gzip` or `br`
    /// for compression. It is ignored if unsupported.
    #[gen_stub(default = WriteOptions::default().content_encoding)]
    pub content_encoding: Option<String>,

    /// Set custom user metadata for the object.
    ///
    /// This attaches user-defined key-value pairs to the object. Be aware
    /// that services may have limits on key/value length and total size.
    /// It is ignored if unsupported.
    #[gen_stub(default = WriteOptions::default().user_metadata)]
    pub user_metadata: Option<HashMap<String, String>>,

    /// Set the `If-Match` condition for the write.
    ///
    /// The write will only succeed if the target's ETag matches the given
    /// value. Use `*` to match any existing object. This helps prevent
    /// overwriting concurrent changes.
    #[gen_stub(default = WriteOptions::default().if_match)]
    pub if_match: Option<String>,

    /// Set the `If-None-Match` condition for the write.
    ///
    /// The write will only succeed if the target's ETag does not match the
    /// given value. Use `*` to ensure the object does not already exist.
    /// This is useful for creating new, unique objects.
    #[gen_stub(default = WriteOptions::default().if_none_match)]
    pub if_none_match: Option<String>,

    /// Ensure the write only succeeds if the object does not already exist.
    ///
    /// If `true`, the operation will fail if the path is already occupied.
    /// This provides a simple "create-if-not-exists" check.
    #[gen_stub(default = WriteOptions::default().if_not_exists)]
    pub if_not_exists: bool,

    /// Set the number of concurrent write operations.
    ///
    /// This enables parallel uploads, which can significantly improve
    /// performance for large files, especially over high-latency networks.
    /// It increases memory usage and falls back to sequential writes if
    /// unsupported.
    #[gen_stub(default = WriteOptions::default().concurrent)]
    pub concurrent: usize,

    /// Set the chunk size for buffered or multipart writes.
    ///
    /// Larger chunks can improve performance and reduce API costs but use
    /// more memory. Some services have minimum chunk size requirements. If not
    /// set, an optimal size is chosen by default.
    #[gen_stub(default = WriteOptions::default().chunk)]
    pub chunk: Option<usize>,
}

impl From<ReadOptions> for ocore::options::ReadOptions {
    fn from(opts: ReadOptions) -> Self {
        Self {
            range: opts.make_range(),
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since,
            if_unmodified_since: opts.if_unmodified_since,
            concurrent: opts.concurrent,
            chunk: opts.chunk,
            gap: opts.gap,
            override_content_type: opts.override_content_type,
            override_cache_control: opts.override_cache_control,
            override_content_disposition: opts.override_content_disposition,
        }
    }
}

impl From<ReadOptions> for ocore::options::ReaderOptions {
    fn from(opts: ReadOptions) -> Self {
        Self {
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since,
            if_unmodified_since: opts.if_unmodified_since,
            concurrent: opts.concurrent,
            chunk: opts.chunk,
            gap: opts.gap,
            prefetch: opts.prefetch,
        }
    }
}

impl From<WriteOptions> for ocore::options::WriteOptions {
    fn from(opts: WriteOptions) -> Self {
        Self {
            append: opts.append,
            concurrent: opts.concurrent,
            chunk: opts.chunk,
            content_type: opts.content_type,
            content_disposition: opts.content_disposition,
            cache_control: opts.cache_control,
            content_encoding: opts.content_encoding,
            user_metadata: opts.user_metadata,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_not_exists: opts.if_not_exists,
        }
    }
}

/// Options for list operations.
#[gen_stub_pyclass]
#[pyclass(get_all, set_all, module = "opendal.options")]
#[derive(FromPyObject, Default, Debug)]
pub struct ListOptions {
    /// Set the max results to return per request.
    ///
    /// This controls the `limit` passed to the underlying service. Users can
    /// use this to manage the memory usage of the list operation.
    #[gen_stub(default = ListOptions::default().limit)]
    pub limit: Option<usize>,

    /// Set the key to start listing from.
    ///
    /// The list operation will start after this specified key, allowing for
    /// pagination.
    #[gen_stub(default = ListOptions::default().start_after)]
    pub start_after: Option<String>,

    /// Control whether the list operation is recursive.
    ///
    /// - If `false`, the operation will only list entries under the given path.
    /// - If `true`, the operation will list all entries that start with the
    ///   given path.
    #[gen_stub(default = ListOptions::default().recursive)]
    pub recursive: bool,

    /// Control whether to return object versions.
    ///
    /// - If `false`, the operation will not return object versions.
    /// - If `true`, the operation will return object versions if the service
    ///   supports it.
    #[gen_stub(default = ListOptions::default().versions)]
    pub versions: bool,

    /// Control whether to return deleted objects.
    ///
    /// - If `false`, the operation will not return deleted objects.
    /// - If `true`, the operation will return deleted objects if the service
    ///   supports object versioning.
    #[gen_stub(default = ListOptions::default().deleted)]
    pub deleted: bool,
}

impl From<ListOptions> for ocore::options::ListOptions {
    fn from(opts: ListOptions) -> Self {
        Self {
            limit: opts.limit,
            start_after: opts.start_after,
            recursive: opts.recursive,
            versions: opts.versions,
            deleted: opts.deleted,
        }
    }
}

/// Options for stat operations.
#[gen_stub_pyclass]
#[pyclass(get_all, set_all, module = "opendal.options")]
#[derive(FromPyObject, Default, Debug)]
pub struct StatOptions {
    /// Set `version` for this operation.
    ///
    /// This option can be used to retrieve the data of a specified version
    /// of the given path.
    ///
    /// If the version does not exist, an error will be returned.
    #[gen_stub(default = StatOptions::default().version)]
    pub version: Option<String>,

    /// Set `if_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` matches the
    /// given `ETag`.
    ///
    /// If the file exists and its ETag does not match, an error will be
    /// returned.
    #[gen_stub(default = StatOptions::default().if_match)]
    pub if_match: Option<String>,

    /// Set `if_none_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` does not match
    /// the given `ETag`.
    ///
    /// If the file exists and its ETag matches, an error will be returned.
    #[gen_stub(default = StatOptions::default().if_none_match)]
    pub if_none_match: Option<String>,

    /// Set `if_modified_since` for this operation.
    ///
    /// This option can be used to check if the file has been modified since
    /// the given timestamp.
    ///
    /// If the file exists and has not been modified since the specified time,
    /// an error will be returned.
    #[gen_stub(default = StatOptions::default().if_modified_since)]
    pub if_modified_since: Option<Timestamp>,

    /// Set `if_unmodified_since` for this operation.
    ///
    /// This feature can be used to check if the file has not been modified
    /// since the given timestamp.
    ///
    /// If the file exists and has been modified since the specified time, an
    /// error will be returned.
    #[gen_stub(default = StatOptions::default().if_unmodified_since)]
    pub if_unmodified_since: Option<Timestamp>,

    /// Specify the content-type header to be sent back by the operation.
    ///
    /// This option is only meaningful when used with presign operations.
    #[gen_stub(default = StatOptions::default().override_content_type)]
    pub override_content_type: Option<String>,

    /// Specify the `cache-control` header to be sent back by the operation.
    ///
    /// This option is only meaningful when used with presign operations.
    #[gen_stub(default = StatOptions::default().override_cache_control)]
    pub override_cache_control: Option<String>,

    /// Specify the `content-disposition` header to be sent back.
    ///
    /// This option is only meaningful when used with presign operations.
    #[gen_stub(default = StatOptions::default().override_content_disposition)]
    pub override_content_disposition: Option<String>,
}

impl From<StatOptions> for ocore::options::StatOptions {
    fn from(opts: StatOptions) -> Self {
        Self {
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since,
            if_unmodified_since: opts.if_unmodified_since,
            override_content_type: opts.override_content_type,
            override_cache_control: opts.override_cache_control,
            override_content_disposition: opts.override_content_disposition,
        }
    }
}
