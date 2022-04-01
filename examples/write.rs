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
use futures::io;
use opendal::Operator;
use opendal_test::services::fs;

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

    // Write from slice.
    let _ = op
        .object("test_file")
        .write_from_slice("Hello, World!")
        .await?;

    // Or write data via a writer.
    let mut w = op.object("test_file").writer(1024).await?;
    let mut r = io::Cursor::new(vec![0; 1024]);
    io::copy(&mut r, &mut w).await?;

    Ok(())
}
