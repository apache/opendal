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

//! Compare streaming reads through Fs and USRBIO on the same 3FS file.
#[cfg(target_os = "linux")]
use std::time::Instant;

#[cfg(target_os = "linux")]
use futures::TryStreamExt;
#[cfg(target_os = "linux")]
use opendal_core::{Operator, Result};
#[cfg(target_os = "linux")]
use opendal_service_hf3fs::Hf3fs;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("The 3FS benchmark requires Linux.");
}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> Result<()> {
    let root = std::env::var("OPENDAL_HF3FS_ROOT").expect("set OPENDAL_HF3FS_ROOT");
    let directory = tempfile::tempdir_in(root).expect("create benchmark directory");
    let root = directory.path().to_str().unwrap();
    let mut builder = Hf3fs::default().root(root);
    if let Ok(library) = std::env::var("OPENDAL_HF3FS_LIBRARY_PATH") {
        builder = builder.library_path(&library);
    }
    let hf3fs = Operator::new(builder)?;
    let fs = Operator::new(opendal_service_fs::Fs::default().root(root))?;
    fs.write("data", vec![0x5a; 64 * 1024 * 1024]).await?;

    for concurrent in [1, 16] {
        for (name, operator) in [("fs", &fs), ("hf3fs", &hf3fs)] {
            // Warm the same file before measuring four complete streaming reads.
            read(operator, concurrent).await?;
            let start = Instant::now();
            let mut bytes = 0;
            for _ in 0..4 {
                bytes += read(operator, concurrent).await?;
            }
            println!(
                "{name} concurrency={concurrent}: {:.1} MiB/s",
                bytes as f64 / start.elapsed().as_secs_f64() / 1048576.0
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn read(operator: &Operator, concurrent: usize) -> Result<usize> {
    let mut stream = operator
        .reader_with("data")
        .chunk(1024 * 1024)
        .concurrent(concurrent)
        .await?
        .into_stream(..)
        .await?;
    let mut size = 0;
    while let Some(buffer) = stream.try_next().await? {
        size += buffer.len();
        std::hint::black_box(buffer);
    }
    Ok(size)
}
