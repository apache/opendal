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

use std::fmt::Debug;
use std::net::Ipv4Addr;
use std::time::Duration;

use anyhow::{Context, Result};
use libunftp::ServerBuilder;
use opendal::{Operator, services};
use suppaftp::Status;
use suppaftp::tokio::AsyncFtpStream;
use suppaftp::types::{FileType, FtpError, FtpResult};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::time::timeout;
use unftp_sbe_opendal::OpendalStorage;

const SESSION_TIMEOUT: Duration = Duration::from_secs(15);

async fn with_ftp_client(test: impl AsyncFnOnce(&mut AsyncFtpStream) -> Result<()>) -> Result<()> {
    let root = tempdir()?;
    let fs = services::Fs::default().root(root.path().to_str().context("non-UTF-8 test root")?);
    let storage = OpendalStorage::new(Operator::new(fs)?);
    let server = ServerBuilder::new(Box::new(move || storage.clone()))
        .passive_ports(0..=0)
        .build()?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;

    let serve = async {
        let (stream, _) = listener.accept().await?;
        server.service(stream).await?;
        Ok::<(), anyhow::Error>(())
    };
    let session = async {
        let mut ftp = AsyncFtpStream::connect(addr)
            .await
            .context("connect to FTP server")?;
        ftp.login("anonymous", "anonymous")
            .await
            .context("FTP login")?;
        ftp.transfer_type(FileType::Binary)
            .await
            .context("TYPE I")?;
        test(&mut ftp).await?;
        ftp.quit().await.context("QUIT")
    };

    let (client_result, server_result) = tokio::join!(
        timeout(SESSION_TIMEOUT, session),
        timeout(SESSION_TIMEOUT, serve),
    );
    client_result.context("FTP client session timed out")??;
    server_result.context("FTP server session timed out")?
}

async fn retrieve(ftp: &mut AsyncFtpStream, path: &str) -> Result<Vec<u8>> {
    let mut stream = ftp
        .retr_as_stream(path)
        .await
        .with_context(|| format!("RETR {path}"))?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    ftp.finalize_retr_stream(stream).await?;
    Ok(bytes)
}

fn assert_reply<T: Debug>(result: FtpResult<T>, expected: Status, command: &str) {
    match result {
        Err(FtpError::UnexpectedResponse(response)) => {
            assert_eq!(response.status, expected, "{command}: {response}");
        }
        other => panic!(
            "{command}: expected FTP status {}, got {other:?}",
            expected.code()
        ),
    }
}

#[tokio::test]
async fn file_lifecycle() -> Result<()> {
    with_ftp_client(async |ftp| {
        let original = "original.bin";
        let renamed = "renamed.bin";
        // Use distinct binary chunks so a truncated or repeated chunk fails comparison.
        let payload: Vec<u8> = (0_u32..16_385).flat_map(u32::to_le_bytes).collect();

        ftp.put_file(original, &mut payload.as_slice())
            .await
            .context("STOR original.bin")?;
        assert_eq!(
            ftp.size(original).await.context("SIZE original.bin")?,
            payload.len()
        );
        assert_eq!(retrieve(ftp, original).await?, payload);
        assert_eq!(
            ftp.nlst(Some(original))
                .await
                .context("NLST original.bin")?,
            [original]
        );

        ftp.rename(original, renamed).await.context("RNFR/RNTO")?;
        assert_eq!(retrieve(ftp, renamed).await?, payload);
        assert_reply(
            ftp.size(original).await,
            Status::FileUnavailable,
            "SIZE original.bin after rename",
        );

        ftp.rm(renamed).await.context("DELE renamed.bin")?;
        assert_reply(
            ftp.size(renamed).await,
            Status::FileUnavailable,
            "SIZE renamed.bin after delete",
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn utf8_directory_lifecycle() -> Result<()> {
    with_ftp_client(async |ftp| {
        let dir = "données";
        let original = "été.bin";
        let renamed = "hiver❄.bin";
        let payload = b"\x00\xff\r\n";

        ftp.mkdir(dir).await.context("MKD")?;
        ftp.cwd(dir).await.context("CWD into directory")?;
        assert_eq!(ftp.pwd().await.context("PWD")?, format!("/{dir}"));
        ftp.put_file(original, &mut payload.as_slice())
            .await
            .context("STOR UTF-8 file")?;
        assert_eq!(
            ftp.size(original).await.context("SIZE UTF-8 file")?,
            payload.len()
        );
        assert_eq!(retrieve(ftp, original).await?, payload);
        assert_eq!(
            ftp.nlst(None).await.context("NLST before rename")?,
            [original]
        );

        ftp.rename(original, renamed)
            .await
            .context("RNFR/RNTO UTF-8 file")?;
        assert_eq!(retrieve(ftp, renamed).await?, payload);
        assert_eq!(
            ftp.nlst(None).await.context("NLST after rename")?,
            [renamed]
        );
        ftp.rm(renamed).await.context("DELE UTF-8 file")?;
        assert!(
            ftp.nlst(None)
                .await
                .context("NLST after delete")?
                .is_empty()
        );

        ftp.cwd("/").await.context("CWD to root")?;
        ftp.rmdir(dir).await.context("RMD")?;
        assert_reply(
            ftp.cwd(dir).await,
            Status::FileUnavailable,
            "CWD after directory removal",
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn append_extends_existing_file() -> Result<()> {
    with_ftp_client(async |ftp| {
        let file = "append.bin";

        ftp.put_file(file, &mut "abc".as_bytes())
            .await
            .context("STOR append.bin")?;
        ftp.append_file(file, &mut "def".as_bytes())
            .await
            .context("APPE append.bin")?;
        assert_eq!(retrieve(ftp, file).await?, b"abcdef");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn cwd_rejects_regular_file() -> Result<()> {
    with_ftp_client(async |ftp| {
        let file = "file.bin";

        ftp.put_file(file, &mut "abc".as_bytes())
            .await
            .context("STOR file.bin")?;
        assert_reply(ftp.cwd(file).await, Status::FileUnavailable, "CWD file.bin");
        assert_eq!(ftp.pwd().await.context("PWD")?, "/");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn restart_is_unsupported() -> Result<()> {
    with_ftp_client(async |ftp| {
        assert_reply(
            ftp.resume_transfer(1).await,
            Status::NotImplemented,
            "REST 1",
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn parent_traversal_delete_errors_and_dir_rename() -> Result<()> {
    with_ftp_client(async |ftp| {
        ftp.put_file("top.txt", &mut "top".as_bytes())
            .await
            .context("STOR top.txt")?;
        ftp.mkdir("dir").await.context("MKD dir")?;
        ftp.cwd("dir").await.context("CWD dir")?;
        ftp.cwd("..").await.context("CWD ..")?;
        assert_eq!(retrieve(ftp, "top.txt").await?, b"top");

        assert_reply(
            ftp.rm("missing.txt").await,
            Status::FileUnavailable,
            "DELE missing.txt",
        );
        assert_reply(ftp.rm("dir").await, Status::FileUnavailable, "DELE dir");

        ftp.put_file("dir/item.txt", &mut "item".as_bytes())
            .await
            .context("STOR dir/item.txt")?;
        ftp.rename("dir/", "dir2/")
            .await
            .context("RNFR dir/ + RNTO dir2/")?;
        assert_eq!(retrieve(ftp, "dir2/item.txt").await?, b"item");
        Ok(())
    })
    .await
}
