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
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use libunftp::ServerBuilder;
use opendal::{Operator, services};
use suppaftp::Status;
use suppaftp::list::File;
use suppaftp::tokio::AsyncFtpStream;
use suppaftp::types::{FileType, FtpError, FtpResult};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::time::timeout;
use unftp_sbe_opendal::OpendalStorage;

const SESSION_TIMEOUT: Duration = Duration::from_secs(15);

async fn with_ftp_client_for(
    op: Operator,
    test: impl AsyncFnOnce(&mut AsyncFtpStream) -> Result<()>,
) -> Result<()> {
    let storage = OpendalStorage::new(op);
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

async fn with_ftp_client(test: impl AsyncFnOnce(&mut AsyncFtpStream) -> Result<()>) -> Result<()> {
    let root = tempdir()?;
    let fs = services::Fs::default().root(root.path().to_str().context("non-UTF-8 test root")?);
    with_ftp_client_for(Operator::new(fs)?, test).await
}

/// A modification time counts as real when it is within the last hour: the
/// entry was just written, so a clock far from now means the server invented
/// the time.
fn is_recent(modified: SystemTime) -> bool {
    SystemTime::now()
        .duration_since(modified)
        .is_ok_and(|elapsed| elapsed < Duration::from_secs(3600))
}

fn parse_list_lines(lines: &[String]) -> Result<Vec<File>> {
    lines
        .iter()
        .map(|line| {
            line.parse::<File>()
                .with_context(|| format!("parse LIST line {line:?}"))
        })
        .collect()
}

async fn retrieve(ftp: &mut AsyncFtpStream, path: &str) -> Result<Vec<u8>> {
    let mut stream = ftp
        .retr_as_stream(path)
        .await
        .with_context(|| format!("RETR {path}"))?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    stream.finish().await?;
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
        ftp.resume_transfer(1).await?;
        assert_reply(
            ftp.retr_as_stream("missing").await.map(|_| ()),
            Status::NotImplemented,
            "REST 1",
        );
        Ok(())
    })
    .await
}

/// STAT replies frame each listing line with the reply code (`200-` on the
/// first line, `200 ` on the last). Strip that framing so the lines parse
/// like the raw LIST output on the data connection.
fn stat_lines(body: &str) -> Vec<String> {
    body.lines()
        .map(|l| {
            let b = l.as_bytes();
            if b.len() >= 4
                && b[..3].iter().all(|c| c.is_ascii_digit())
                && (b[3] == b'-' || b[3] == b' ')
            {
                l[4..].to_string()
            } else {
                l.to_string()
            }
        })
        .filter(|l| !l.is_empty())
        .collect()
}

/// Entries without a modification time stay listable on Memory: files and
/// directories written there carry no `last_modified`, so `LIST` and `STAT`
/// must still parse, while `MDTM` and `MLST` must not invent a time.
#[tokio::test]
async fn missing_mtime_entries_on_memory() -> Result<()> {
    let op = Operator::new(services::Memory::default())?;
    op.write("f.txt", "hello").await?;
    op.create_dir("d/").await?;
    with_ftp_client_for(op, async |ftp| {
        let lines = ftp.list(None).await.context("LIST")?;
        let files = parse_list_lines(&lines)?;
        assert_eq!(files.len(), 2, "LIST lines: {lines:?}");
        let file = files
            .iter()
            .find(|f| f.name() == "f.txt")
            .context("f.txt in LIST")?;
        assert!(file.is_file());
        assert_eq!(file.size(), 5);
        let dir = files
            .iter()
            .find(|f| f.name() == "d")
            .context("d in LIST")?;
        assert!(dir.is_directory());
        assert!(
            lines
                .iter()
                .all(|line| !line.contains("1970") && !line.contains("--- -- --:--")),
            "LIST lines must neither invent a time nor use the unparsable placeholder: {lines:?}"
        );

        for path in ["f.txt", "d"] {
            let response = ftp
                .custom_command(format!("STAT /{path}"), &[Status::CommandOk])
                .await
                .with_context(|| format!("STAT /{path}"))?;
            let body = String::from_utf8_lossy(&response.body);
            let name = path.trim_end_matches('/');
            assert!(
                stat_lines(&body)
                    .iter()
                    .any(|line| line.parse::<File>().is_ok_and(|f| f.name() == name)),
                "STAT /{path} must return a parseable line for {name}: {body}"
            );
            assert!(
                !body.contains("1970"),
                "STAT /{path} must not invent a time: {body}"
            );
        }

        assert_reply(ftp.mdtm("f.txt").await, Status::ActionAborted, "MDTM f.txt");
        assert_reply(ftp.mdtm("d").await, Status::ActionAborted, "MDTM d");

        for (path, kind) in [("f.txt", "type=file"), ("d", "type=dir")] {
            let facts = ftp
                .mlst(Some(path))
                .await
                .with_context(|| format!("MLST {path}"))?;
            assert!(facts.contains(kind), "MLST {path} must say {kind}: {facts}");
            assert!(
                !facts.contains("modify="),
                "MLST {path} must not invent a time: {facts}"
            );
        }

        ftp.cwd("d").await.context("CWD d")?;
        Ok(())
    })
    .await
}

/// Entries with a known modification time keep the `ls`-style listing and
/// report their real time through `MDTM` and `MLST`.
#[tokio::test]
async fn known_mtime_entries_on_fs() -> Result<()> {
    with_ftp_client(async |ftp| {
        ftp.put_file("f.txt", &mut "hello".as_bytes())
            .await
            .context("STOR f.txt")?;
        ftp.mkdir("d").await.context("MKD d")?;

        let lines = ftp.list(None).await.context("LIST")?;
        let files = parse_list_lines(&lines)?;
        let file = files
            .iter()
            .find(|f| f.name() == "f.txt")
            .context("f.txt in LIST")?;
        assert!(file.is_file());
        assert!(is_recent(file.modified()), "f.txt mtime must be recent");
        let dir = files
            .iter()
            .find(|f| f.name() == "d")
            .context("d in LIST")?;
        assert!(dir.is_directory());
        assert!(is_recent(dir.modified()), "d mtime must be recent");

        let response = ftp
            .custom_command("STAT /f.txt", &[Status::CommandOk])
            .await
            .context("STAT /f.txt")?;
        let body = String::from_utf8_lossy(&response.body);
        assert!(
            stat_lines(&body).iter().any(|line| line
                .parse::<File>()
                .is_ok_and(|f| { f.name() == "f.txt" && is_recent(f.modified()) })),
            "STAT /f.txt must return a parseable line with a recent time: {body}"
        );

        let mdtm = ftp.mdtm("f.txt").await.context("MDTM f.txt")?;
        let mdtm_ts = mdtm.in_tz("UTC")?.timestamp().as_second() as u64;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .context("system time before epoch")?
            .as_secs();
        assert!(
            now - mdtm_ts < 3600,
            "MDTM f.txt must be recent, got {mdtm}"
        );

        let facts = ftp.mlst(Some("f.txt")).await.context("MLST f.txt")?;
        assert!(
            facts.contains("modify="),
            "MLST f.txt must carry modify: {facts}"
        );
        assert!(
            !facts.contains("modify=19700101000000"),
            "MLST f.txt must not report the epoch: {facts}"
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
