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
use anyhow::Result;
use futures::AsyncSeek;
use futures::{io, AsyncSeekExt};
use futures::{AsyncRead, AsyncReadExt};
use opendal::{ObjectMode, Operator};
use opendal_test::services::fs;
use std::io::SeekFrom;

#[tokio::main]
async fn main() -> Result<()> {
    // Using opendal internal test framework for example.
    // Don't use this in production.
    // Please init your backend via related example instead.
    let acc = fs::new().await?;
    if acc.is_none() {
        return Ok(());
    }
    let op = Operator::new(acc.unwrap());

    // Real example starts from here.

    // Get metadata of an object.
    let meta = op.object("test_file").metadata().await?;
    println!("path: {}", meta.path());
    println!("mode: {:?}", meta.mode());
    println!("content_length: {}", meta.content_length());

    // Use mode to check whether this object is a file or dir.
    let meta = op.object("test_file").metadata().await?;
    match meta.mode() {
        ObjectMode::FILE => {
            println!("Handle a file")
        }
        ObjectMode::DIR => {
            println!("Handle a dir")
        }
        ObjectMode::Unknown => {
            println!("Handle unknown")
        }
    }
    println!("path: {}", meta.path());
    println!("mode: {}", meta.mode());
    println!("content_length: {}", meta.content_length());

    Ok(())
}
