// Copyright 2022 Datafuse Labs.
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

//! Providing IO utils like `into_sink`, `into_stream`.

mod into_stream;
pub use into_stream::into_stream;

mod into_sink;
pub use into_sink::into_sink;

mod into_reader;
pub use into_reader::into_reader;

mod into_writer;
pub use into_writer::into_writer;

mod stream_observer;
pub use stream_observer::observe_stream;
pub use stream_observer::StreamEvent;
pub use stream_observer::StreamObserver;

mod sink_observer;
pub use sink_observer::observe_sink;
pub use sink_observer::SinkEvent;
pub use sink_observer::SinkObserver;

mod http_body;
pub use http_body::new_http_channel;
pub use http_body::HttpBodySinker;
pub use http_body::HttpBodyWriter;

mod seekable_reader;
pub use seekable_reader::seekable_read;
