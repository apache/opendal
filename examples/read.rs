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
use futures::AsyncReadExt;
use futures::{io, AsyncSeekExt};
use opendal::Operator;
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

    // Get a while file reader.
    let mut r = op.object("test_file").reader();
    io::copy(&mut r, &mut io::sink()).await?;

    // Get file reader in range [1024, 2048).
    let mut r = op.object("test_file").range_reader(1024, 1024);
    io::copy(&mut r, &mut io::sink()).await?;

    // Get a file reader from offset 1024.
    let mut r = op.object("test_file").offset_reader(1024);
    io::copy(&mut r, &mut io::sink()).await?;

    // Get a file reader inside limit 1024.
    let mut r = op.object("test_file").limited_reader(1024);
    io::copy(&mut r, &mut io::sink()).await?;

    // Our reader implement `futures::AsyncRead`.
    let mut r = op.object("test_file").reader();
    r.seek(SeekFrom::End(-1024)).await?;
    r.read_exact(&mut vec![0; 1024]).await?;

    Ok(())
}
