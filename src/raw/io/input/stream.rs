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

use std::io::Result;

use bytes::Bytes;

/// Stream represents a stream of bytes.
///
/// This trait is used as alias to `futures::Stream<Item = Result<Bytes>> + Unpin + Send`.
pub trait Stream: futures::Stream<Item = Result<Bytes>> + Unpin + Send + Sync {}
impl<T> Stream for T where T: futures::Stream<Item = Result<Bytes>> + Unpin + Send + Sync {}

/// Streamer is a boxed dyn [`Stream`].
pub type Streamer = Box<dyn Stream>;
