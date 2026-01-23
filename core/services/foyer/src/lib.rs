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

//! Foyer service implementation for Apache OpenDAL.
//!
//! Foyer is a high-performance hybrid cache library that supports both
//! in-memory and on-disk caching. This service provides foyer as a
//! volatile KV storage backend, similar to using Redis as a cache.
//!
//! Note: Data stored in foyer may be evicted when the cache is full.
//! Do not use this service for persistent storage.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

mod backend;
mod config;
mod core;
mod deleter;
mod writer;

use std::ops::Deref;

use foyer::Code;
use foyer::CodeError;

use opendal_core::Buffer;

pub use backend::FoyerBuilder as Foyer;
pub use config::FoyerConfig;

/// Default scheme for foyer service.
pub const FOYER_SCHEME: &str = "foyer";

/// [`FoyerKey`] is a key for the foyer cache.
///
/// It uses bincode (via serde) for efficient serialization.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FoyerKey {
    /// The path of the key.
    pub path: String,
}

/// [`FoyerValue`] is a wrapper around `Buffer` that implements the `Code` trait.
#[derive(Debug)]
pub struct FoyerValue(pub Buffer);

impl Deref for FoyerValue {
    type Target = Buffer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Code for FoyerValue {
    fn encode(&self, writer: &mut impl std::io::Write) -> std::result::Result<(), CodeError> {
        let len = self.0.len() as u64;
        writer.write_all(&len.to_le_bytes())?;
        std::io::copy(&mut self.0.clone(), writer)?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, CodeError>
    where
        Self: Sized,
    {
        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;
        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer[..len])?;
        Ok(FoyerValue(buffer.into()))
    }

    fn estimated_size(&self) -> usize {
        8 + self.0.len()
    }
}

/// Register this service into the given registry.
///
/// Note: Foyer service requires a pre-configured HybridCache, so it cannot
/// be registered for URI-based construction. Use `Foyer::new(cache)` instead.
pub fn register_foyer_service(_registry: &opendal_core::OperatorRegistry) {
    // Foyer requires a HybridCache instance that cannot be constructed from config alone.
    // Users should use Foyer::new(cache) directly instead of URI-based construction.
}

#[cfg(test)]
mod tests {
    use foyer::{
        DirectFsDeviceOptions, Engine, HybridCacheBuilder, LargeEngineOptions, RecoverMode,
    };
    use opendal_core::Operator;
    use size::consts::MiB;

    use super::*;

    fn key(i: u8) -> String {
        format!("obj-{i}")
    }

    fn value(i: u8) -> Vec<u8> {
        vec![i; 1024]
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let dir = tempfile::tempdir().unwrap();

        let cache = HybridCacheBuilder::new()
            .memory(10)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Foyer::new(cache.clone())).unwrap().finish();

        // Write some data
        for i in 0..10 {
            op.write(&key(i), value(i)).await.unwrap();
        }

        // Read back
        for i in 0..10 {
            let buf = op.read(&key(i)).await.unwrap();
            assert_eq!(buf.to_vec(), value(i));
        }

        // Stat
        for i in 0..10 {
            let meta = op.stat(&key(i)).await.unwrap();
            assert_eq!(meta.content_length(), 1024);
        }

        // Delete
        for i in 0..10 {
            op.delete(&key(i)).await.unwrap();
        }

        // Verify deleted
        for i in 0..10 {
            let res = op.read(&key(i)).await;
            assert!(res.is_err(), "should fail to read deleted file");
        }
    }

    #[tokio::test]
    async fn test_range_read() {
        let dir = tempfile::tempdir().unwrap();

        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Foyer::new(cache)).unwrap().finish();

        let data: Vec<u8> = (0..100).collect();
        op.write("test", data.clone()).await.unwrap();

        // Range read
        let buf = op.read_with("test").range(10..20).await.unwrap();
        assert_eq!(buf.to_vec(), data[10..20]);
    }
}
