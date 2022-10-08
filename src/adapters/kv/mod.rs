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

//! Providing Key Value Adapter for OpenDAL.
//!
//! Any services that implement `Adapter` can be used an OpenDAL Service.
//!
//! # Notes
//!
//! This adapter creates a new storage format which is not stable.
//!
//! Any service that built upon this adapter should not be persisted.

mod api;
pub use api::Adapter;
pub use api::KeyStreamer;
pub use api::Metadata;
use api::BLOCK_SIZE;
use api::INODE_ROOT;

mod backend;
pub use backend::Backend;

mod key;
pub use key::next_prefix;
use key::Key;
