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
use std::io::SeekFrom;
use std::str::from_utf8;

use anyhow::Result;
use bytes::Bytes;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;

use crate::services::fs;
use crate::services::memory;
use crate::Operator;

#[tokio::test]
async fn test_reader() -> Result<()> {
    let f = Operator::new(fs::Backend::build().finish().await.unwrap());

    let path = format!("/tmp/{}", uuid::Uuid::new_v4());

    // Create a test file.
    let x = f
        .object(&path)
        .writer()
        .write_bytes("Hello, world!".to_string().into_bytes())
        .await
        .unwrap();
    assert_eq!(x, 13);

    let mut r = f.object(&path).reader();

    // Seek to offset 3.
    let n = r.seek(SeekFrom::Start(3)).await?;
    assert_eq!(n, 3);

    // Read only one byte.
    let mut bs = Vec::new();
    bs.resize(1, 0);
    let n = r.read(&mut bs).await?;
    assert_eq!("l", from_utf8(&bs).unwrap());
    assert_eq!(n, 1);
    let n = r.seek(SeekFrom::Current(0)).await?;
    assert_eq!(n, 4);

    // Seek to end.
    let n = r.seek(SeekFrom::End(-1)).await?;
    assert_eq!(n, 12);

    // Read only one byte.
    let mut bs = Vec::new();
    bs.resize(1, 0);
    let n = r.read(&mut bs).await?;
    assert_eq!("!", from_utf8(&bs)?);
    assert_eq!(n, 1);
    let n = r.seek(SeekFrom::Current(0)).await?;
    assert_eq!(n, 13);

    Ok(())
}

#[tokio::test]
async fn test_range_reader() -> Result<()> {
    let f = Operator::new(fs::Backend::build().finish().await.unwrap());

    let path = format!("/tmp/{}", uuid::Uuid::new_v4());

    // Create a test file.
    let x = f
        .object(&path)
        .writer()
        .write_bytes("Hello, world!".to_string().into_bytes())
        .await
        .unwrap();
    assert_eq!(x, 13);

    let mut r = f.object(&path).range_reader(1, 10);
    let mut buf = vec![];
    let n = r.read_to_end(&mut buf).await?;
    assert_eq!(n, 10);
    assert_eq!("ello, worl", from_utf8(&buf).unwrap());

    let n = r.seek(SeekFrom::Start(0)).await?;
    assert_eq!(n, 0);
    let n = r.seek(SeekFrom::End(0)).await?;
    assert_eq!(n, 10);

    Ok(())
}

#[tokio::test]
async fn test_offset_reader() -> Result<()> {
    let f = Operator::new(fs::Backend::build().finish().await.unwrap());

    let path = format!("/tmp/{}", uuid::Uuid::new_v4());

    // Create a test file.
    let x = f
        .object(&path)
        .writer()
        .write_bytes("Hello, world!".to_string().into_bytes())
        .await
        .unwrap();
    assert_eq!(x, 13);

    let mut r = f.object(&path).offset_reader(1);
    let mut buf = vec![];
    let n = r.read_to_end(&mut buf).await?;
    assert_eq!(n, 12);
    assert_eq!("ello, world!", from_utf8(&buf).unwrap());

    let n = r.seek(SeekFrom::Start(0)).await?;
    assert_eq!(n, 0);
    let n = r.seek(SeekFrom::End(0)).await?;
    assert_eq!(n, 12);

    Ok(())
}

#[tokio::test]
async fn test_limited_reader() -> Result<()> {
    let f = Operator::new(fs::Backend::build().finish().await.unwrap());

    let path = format!("/tmp/{}", uuid::Uuid::new_v4());

    // Create a test file.
    let x = f
        .object(&path)
        .writer()
        .write_bytes("Hello, world!".to_string().into_bytes())
        .await
        .unwrap();
    assert_eq!(x, 13);

    let mut r = f.object(&path).limited_reader(5);
    let mut buf = vec![];
    let n = r.read_to_end(&mut buf).await?;
    assert_eq!(n, 5);
    assert_eq!("Hello", from_utf8(&buf).unwrap());

    let n = r.seek(SeekFrom::Start(0)).await?;
    assert_eq!(n, 0);
    let n = r.seek(SeekFrom::End(0)).await?;
    assert_eq!(n, 5);

    Ok(())
}

#[tokio::test]
async fn test_memory_reader() -> Result<()> {
    let data = "Hello, world!";
    let f = Operator::new(
        memory::Backend::build()
            .data(Bytes::from(data))
            .finish()
            .await
            .unwrap(),
    );
    let mut r = f.object("").limited_reader(5);
    let mut buf = vec![];

    let n = r.read_to_end(&mut buf).await?;
    assert_eq!(n, 5);
    assert_eq!("Hello", from_utf8(&buf).unwrap());

    let n = r.seek(SeekFrom::Start(0)).await?;
    assert_eq!(n, 0);
    let n = r.seek(SeekFrom::End(0)).await?;
    assert_eq!(n, 5);

    Ok(())
}
