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

use std::fmt::Debug;
use std::io::Result;

use async_trait::async_trait;
use futures::Stream;

use crate::Scheme;

/// KvAdapter is the adapter to underlying kv services.
///
/// By implement this trait, any kv service can work as an OpenDAL Service.
#[async_trait]
pub trait Adapter: Send + Sync + Debug + Clone + 'static {
    /// Return the medata of this key value accessor.
    fn metadata(&self) -> Metadata;
    /// Get a key from service.
    ///
    /// - return `Ok(None)` if this key is not exist.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Set a key into service.
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()>;
    /// Scan a range of keys.
    async fn scan(&self, prefix: &[u8]) -> Result<KeyStreamer>;
    /// Delete a key from service.
    ///
    /// - return `Ok(())` even if this key is not exist.
    async fn delete(&self, key: &[u8]) -> Result<()>;
}

/// Use 64 KiB as a block.
pub const BLOCK_SIZE: usize = 64 * 1024;

/// OpenDAL will reserve all inode between 0~16.
pub const INODE_ROOT: u64 = 16;
pub const INODE_START: u64 = INODE_ROOT + 1;

#[derive(bincode::Encode, bincode::Decode)]
pub struct Meta {
    /// First valid inode will start from `17`.
    next_inode: u64,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            next_inode: INODE_START,
        }
    }
}

impl Meta {
    /// Get and update next inode.
    pub fn next_inode(&mut self) -> u64 {
        let inode = self.next_inode;
        self.next_inode += 1;
        inode
    }
}

/// Metadata for this key value accessor.
pub struct Metadata {
    scheme: Scheme,
    name: String,
}

impl Metadata {
    /// Create a new KeyValueAccessorMetadata.
    pub fn new(scheme: Scheme, name: &str) -> Self {
        Self {
            scheme,
            name: name.to_string(),
        }
    }

    /// Get the scheme.
    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    /// Get the name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// KeyStream represents a stream of keys.
pub trait KeyStream: Stream<Item = Result<Vec<u8>>> + Unpin + Send {}
impl<T> KeyStream for T where T: Stream<Item = Result<Vec<u8>>> + Unpin + Send {}

/// KeyValueStreamer is a boxed dyn [`KeyValueStream`]
pub type KeyStreamer = Box<dyn KeyStream>;
