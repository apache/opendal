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
pub use api::ReadOperation;
pub use api::Reader;

// mod into_streamable_read;
// pub use into_streamable_read::into_streamable_read;
// pub use into_streamable_read::StreamableReader;

// mod range_read;
// pub use range_read::RangeReader;

// mod file_read;
// pub use file_read::FileReader;

// mod into_read_from_stream;
// pub use into_read_from_stream::into_read_from_stream;
// pub use into_read_from_stream::FromStreamReader;

// mod futures_read;
// pub use futures_read::FuturesReader;

// mod tokio_read;
// pub use tokio_read::TokioReader;

// mod std_read;
// pub use std_read::StdReader;

// mod lazy_read;
// pub use lazy_read::LazyReader;

// mod buffer_reader;
// pub use buffer_reader::BufferReader;
