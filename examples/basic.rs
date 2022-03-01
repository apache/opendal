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
use futures::StreamExt;
use opendal::services::fs;
use opendal::ObjectMode;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let op = Operator::new(fs::Backend::build().root("/tmp").finish().await?);

    let o = op.object("test_file");

    // Write data info file;
    let w = o.writer();
    let n = w
        .write_bytes("Hello, World!".to_string().into_bytes())
        .await?;
    assert_eq!(n, 13);

    // Read data from file;
    let mut r = o.reader();
    let mut buf = vec![];
    let n = r.read_to_end(&mut buf).await?;
    assert_eq!(n, 13);
    assert_eq!(String::from_utf8_lossy(&buf), "Hello, World!");

    // Get file's Metadata
    let meta = o.metadata().await?;
    assert_eq!(meta.content_length(), 13);

    // List current dir.
    let mut obs = op.objects("").map(|o| o.expect("list object"));
    let mut found = false;
    while let Some(o) = obs.next().await {
        let meta = o.metadata().await?;
        if meta.path().contains("test_file") {
            let mode = meta.mode();
            assert!(mode.contains(ObjectMode::FILE));

            found = true
        }
    }
    assert!(found, "tset_file should be found in iterator");

    // Delete file.
    o.delete().await?;

    Ok(())
}
