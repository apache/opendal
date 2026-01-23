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

//! Example demonstrating how to use the foyer service as a volatile KV cache.
//!
//! Run with:
//! ```sh
//! cargo run --example foyer_service --features services-foyer
//! ```

use foyer::{DirectFsDeviceOptions, Engine, HybridCacheBuilder, LargeEngineOptions, RecoverMode};
use opendal::services::Foyer;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for foyer's disk cache
    let dir = tempfile::tempdir()?;

    // Build a foyer HybridCache with both memory and disk storage
    let cache = HybridCacheBuilder::new()
        .memory(64 * 1024 * 1024) // 64MB memory cache
        .with_shards(4)
        .storage(Engine::Large(LargeEngineOptions::new()))
        .with_device_options(
            DirectFsDeviceOptions::new(dir.path())
                .with_capacity(256 * 1024 * 1024) // 256MB disk cache
                .with_file_size(64 * 1024 * 1024), // 64MB per file
        )
        .with_recover_mode(RecoverMode::None)
        .build()
        .await?;

    // Create an OpenDAL operator with the foyer service
    let op = Operator::new(Foyer::new(cache))?
        .with_name("my-foyer-cache")
        .finish();

    println!("Foyer service initialized!");

    // Write some data
    op.write("user:123", b"Alice").await?;
    op.write("user:456", b"Bob").await?;
    println!("Written user data to cache");

    // Read data back
    let user_123 = op.read("user:123").await?;
    let user_456 = op.read("user:456").await?;
    println!("user:123 = {}", String::from_utf8_lossy(&user_123));
    println!("user:456 = {}", String::from_utf8_lossy(&user_456));

    // Check if key exists
    let meta = op.stat("user:123").await?;
    println!("user:123 size = {} bytes", meta.content_length());

    // Delete a key
    op.delete("user:456").await?;
    println!("Deleted user:456");

    // Verify deletion
    match op.read("user:456").await {
        Ok(_) => println!("❌ user:456 still exists (unexpected)"),
        Err(_) => println!("✅ user:456 successfully deleted"),
    }

    println!("\n✅ Foyer service example completed!");

    Ok(())
}
