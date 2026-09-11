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

//! Read metadata and stream contents against the GooseFS behavior-test fixture.

use futures::TryStreamExt;
use opendal::{BytesRange, Operator};

#[tokio::test]
async fn read_stream_metadata_describes_the_full_file() -> opendal::Result<()> {
    let Ok(master) = std::env::var("OPENDAL_GOOSEFS_MASTER_ADDR") else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return Ok(());
    };
    let root = std::env::var("OPENDAL_GOOSEFS_ROOT").unwrap_or_else(|_| "/".to_string());
    let op = Operator::new(
        opendal::services::GooseFs::default()
            .master_addr(&master)
            .root(&root)
            .write_type("must_cache"),
    )?;
    let path = format!("read-metadata-{}", std::process::id());
    let data = b"0123456789abcdef";
    op.write(&path, data.as_slice()).await?;
    let expected = op.stat(&path).await?;

    let result = async {
        for (range, selected) in [
            (BytesRange::from(..), 0..16),
            (BytesRange::from(3..8), 3..8),
            (BytesRange::from(3..), 3..16),
            (BytesRange::suffix(5), 11..16),
        ] {
            let mut stream = op.reader(&path).await?.into_bytes_stream(range).await?;
            let metadata = stream.metadata().await?;
            assert_eq!(metadata.content_length(), data.len() as u64);
            assert_eq!(metadata.etag(), expected.etag());
            assert_eq!(metadata.last_modified(), expected.last_modified());

            let mut actual = Vec::new();
            while let Some(chunk) = stream.try_next().await.map_err(|error| {
                opendal::Error::new(opendal::ErrorKind::Unexpected, "read stream failed")
                    .set_source(error)
            })? {
                actual.extend_from_slice(&chunk);
            }
            assert_eq!(actual, &data[selected]);
            assert!(stream.try_next().await.unwrap().is_none());
        }
        Ok(())
    }
    .await;
    op.delete(&path).await?;
    result
}
