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

use async_trait::async_trait;
use std::fmt::Debug;
use std::io::Result;

/// KeyValueAccessor is the accessor to underlying kv services.
///
/// By implement this trait, any kv service can work as an OpenDAL Service.
#[async_trait]
pub trait KeyValueAccessor: Send + Sync + Debug {
    /// Get a key from service.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Set a key into service.
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()>;
    /// Delete a key from service.
    async fn delete(&self, key: &[u8]) -> Result<()>;
}
