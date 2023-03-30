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

//! into_reader will provide different implementations to convert into
//! [`oio::Read`][crate::raw::oio::Read]
//!
//! - (Some(offset), Some(size)) => by_range
//! - (Some(offset), None) => by_offset
//! - (None, Some(size)) => by_tail
//! - (None, None) => by_offset
//!
//! The main different is whether and when to call `stat` to get total
//! content length.
//!
//! # TODO
//!
//! We only implement by_range so far.
//!
//! We should implement other types so that they can be zero cost on non-seek
//! cases.
//!
//! For example, for `by_full`, we don't need to do `stat` everytime. If
//! user call `poll_read` first, we can get the total_size from returning
//! reader. In this way, we can save 40ms in average for every s3 read call.

mod by_range;
pub use by_range::by_range;
pub use by_range::RangeReader;

mod from_fd;
pub use from_fd::from_fd;
pub use from_fd::FdReader;
