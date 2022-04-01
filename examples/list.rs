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
use futures::StreamExt;
use opendal::ObjectMode;
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

    // Start listing a dir.
    let mut obs = op.objects("test_dir").await?;
    // ObjectStream implements `futures::Stream`
    while let Some(o) = obs.next().await {
        let mut o = o?;
        // It's highly possible that OpenDAL already did metadata during list.
        // Use `Object::metadata_cached()` to get cached metadata at first.
        let meta = o.metadata_cached().await?;
        match meta.mode() {
            ObjectMode::FILE => {
                println!("Handling file")
            }
            ObjectMode::DIR => {
                println!("Handling dir like start a new list via meta.path()")
            }
            ObjectMode::Unknown => continue,
        }
    }

    Ok(())
}
