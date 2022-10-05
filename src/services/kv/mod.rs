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

//! KV Service support for OpenDAL.
//!
//! This is not a real service that can be used by users. Service implementer can
//! use this service to build service like `redis`, `memory`, `tikv`.
//!
//! # Note
//!
//! kv service is not stable, please don't use this for persist data.

mod accessor;
pub use accessor::KeyValueAccessor;
pub use accessor::KeyValueAccessorMetadata;
pub use accessor::KeyValueStreamer;

mod backend;
pub use backend::Backend;
pub use backend::Builder;

mod key;
pub use key::ScopedKey;
