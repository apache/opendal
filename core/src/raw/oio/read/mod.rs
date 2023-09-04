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

mod api;
pub use api::BlockingRead;
pub use api::BlockingReader;
pub use api::Read;
pub use api::ReadExt;
pub use api::ReadOperation;
pub use api::Reader;

mod into_streamable_read;
pub use into_streamable_read::into_streamable_read;
pub use into_streamable_read::StreamableReader;

mod into_seekable_read_by_range;
pub use into_seekable_read_by_range::into_seekable_read_by_range;
pub use into_seekable_read_by_range::ByRangeSeekableReader;

mod into_read_from_file;
pub use into_read_from_file::into_read_from_file;
pub use into_read_from_file::FromFileReader;

mod into_read_from_stream;
pub use into_read_from_stream::into_read_from_stream;
pub use into_read_from_stream::FromStreamReader;

mod cloneable_read;
pub use cloneable_read::into_cloneable_reader_within_std;
pub use cloneable_read::into_cloneable_reader_within_tokio;
pub use cloneable_read::CloneableReaderWithinStd;
pub use cloneable_read::CloneableReaderWithinTokio;
