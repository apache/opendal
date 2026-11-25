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

use crate::core::SocketStream;
use opendal_core::raw::*;
use opendal_core::*;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) mod constants {
    pub const OK_STATUS: u16 = 0x0000;
    pub const KEY_NOT_FOUND: u16 = 0x0001;
    pub const AUTH_ERROR: u16 = 0x0020;

    /// Maximum response body size drained for error or non-GET control responses
    /// before treating the connection as poisoned to avoid unbounded allocations.
    pub const MAX_CONTROL_BODY_LENGTH: u32 = 64 * 1024;
}

const HEADER_LENGTH: usize = 24;
const PLAIN_AUTH_MECHANISM: &str = "PLAIN";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Get = 0x00,
    Set = 0x01,
    Delete = 0x04,
    Version = 0x0b,
    StartAuth = 0x21,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Magic {
    Request = 0x80,
    Response = 0x81,
}

#[derive(Debug)]
pub struct StoreExtras {
    pub flags: u32,
    pub expiration: u32,
}

#[derive(Debug, Default)]
pub struct PacketHeader {
    pub magic: u8,
    pub opcode: u8,
    pub key_length: u16,
    pub extras_length: u8,
    pub data_type: u8,
    pub vbucket_id_or_status: u16,
    pub total_body_length: u32,
    pub opaque: u32,
    pub cas: u64,
}

impl PacketHeader {
    pub(super) fn to_bytes(&self) -> [u8; HEADER_LENGTH] {
        let mut buf = [0u8; HEADER_LENGTH];
        buf[0] = self.magic;
        buf[1] = self.opcode;
        buf[2..4].copy_from_slice(&self.key_length.to_be_bytes());
        buf[4] = self.extras_length;
        buf[5] = self.data_type;
        buf[6..8].copy_from_slice(&self.vbucket_id_or_status.to_be_bytes());
        buf[8..12].copy_from_slice(&self.total_body_length.to_be_bytes());
        buf[12..16].copy_from_slice(&self.opaque.to_be_bytes());
        buf[16..24].copy_from_slice(&self.cas.to_be_bytes());
        buf
    }

    pub(super) fn from_bytes(buf: [u8; HEADER_LENGTH]) -> Self {
        Self {
            magic: buf[0],
            opcode: buf[1],
            key_length: u16::from_be_bytes([buf[2], buf[3]]),
            extras_length: buf[4],
            data_type: buf[5],
            vbucket_id_or_status: u16::from_be_bytes([buf[6], buf[7]]),
            total_body_length: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
            opaque: u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]),
            cas: u64::from_be_bytes([
                buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
            ]),
        }
    }

    pub async fn write(self, writer: &mut SocketStream) -> io::Result<()> {
        let buf = self.to_bytes();
        writer.write_all(&buf).await
    }

    pub async fn read(reader: &mut SocketStream) -> io::Result<PacketHeader> {
        let mut buf = [0u8; HEADER_LENGTH];
        reader.read_exact(&mut buf).await?;
        Ok(Self::from_bytes(buf))
    }
}

pub struct Response {
    header: PacketHeader,
    _key: Vec<u8>,
    _extras: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Debug)]
pub struct Connection {
    io: SocketStream,
    next_opaque: u32,
    poisoned: bool,
}

impl Connection {
    pub fn new(io: SocketStream) -> Self {
        Self {
            io,
            next_opaque: 1,
            poisoned: false,
        }
    }

    /// Returns true if an in-flight request was interrupted or a framing/I/O
    /// error left the underlying byte stream in an unknown state.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(
                Error::new(ErrorKind::Unexpected, "memcached connection is poisoned")
                    .set_temporary(),
            );
        }
        Ok(())
    }

    fn allocate_opaque(&mut self) -> u32 {
        let opaque = self.next_opaque;
        self.next_opaque = self.next_opaque.wrapping_add(1);
        if self.next_opaque == 0 {
            self.next_opaque = 1;
        }
        opaque
    }

    pub async fn auth(&mut self, username: &str, password: &str) -> Result<()> {
        self.ensure_healthy()?;

        let key_length = u16::try_from(PLAIN_AUTH_MECHANISM.len()).map_err(|_| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "memcached auth mechanism exceeds binary protocol key length",
            )
        })?;
        let total_body_length =
            encode_auth_body_length(PLAIN_AUTH_MECHANISM.len(), username.len(), password.len())?;

        let opaque = self.allocate_opaque();
        let request_header = PacketHeader {
            magic: Magic::Request as u8,
            opcode: Opcode::StartAuth as u8,
            key_length,
            total_body_length,
            opaque,
            ..Default::default()
        };

        // Mark poisoned before writing so async cancellation or any I/O failure
        // prevents the pool from recycling a desynchronized socket.
        self.poisoned = true;
        request_header
            .write(&mut self.io)
            .await
            .map_err(new_std_io_error)?;
        self.io
            .write_all(PLAIN_AUTH_MECHANISM.as_bytes())
            .await
            .map_err(new_std_io_error)?;
        self.io.write_u8(0).await.map_err(new_std_io_error)?;
        self.io
            .write_all(username.as_bytes())
            .await
            .map_err(new_std_io_error)?;
        self.io.write_u8(0).await.map_err(new_std_io_error)?;
        self.io
            .write_all(password.as_bytes())
            .await
            .map_err(new_std_io_error)?;
        self.io.flush().await.map_err(new_std_io_error)?;

        let response = self.read_response(Opcode::StartAuth, opaque).await?;
        if response.header.vbucket_id_or_status != constants::OK_STATUS {
            return Err(status_error(
                response.header.vbucket_id_or_status,
                &response.value,
            ));
        }
        Ok(())
    }

    pub async fn version(&mut self) -> Result<String> {
        self.ensure_healthy()?;

        let opaque = self.allocate_opaque();
        let request_header = PacketHeader {
            magic: Magic::Request as u8,
            opcode: Opcode::Version as u8,
            opaque,
            ..Default::default()
        };

        self.poisoned = true;
        request_header
            .write(&mut self.io)
            .await
            .map_err(new_std_io_error)?;
        self.io.flush().await.map_err(new_std_io_error)?;

        let response = self.read_response(Opcode::Version, opaque).await?;
        if response.header.vbucket_id_or_status != constants::OK_STATUS {
            return Err(status_error(
                response.header.vbucket_id_or_status,
                &response.value,
            ));
        }

        String::from_utf8(response.value).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "unexpected data received").set_source(e)
        })
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        self.ensure_healthy()?;

        let key_length = encode_key_length(key)?;
        let total_body_length = encode_total_body_length(0, key_length, 0)?;

        let opaque = self.allocate_opaque();
        let request_header = PacketHeader {
            magic: Magic::Request as u8,
            opcode: Opcode::Get as u8,
            key_length,
            total_body_length,
            opaque,
            ..Default::default()
        };

        self.poisoned = true;
        request_header
            .write(&mut self.io)
            .await
            .map_err(new_std_io_error)?;
        self.io
            .write_all(key.as_bytes())
            .await
            .map_err(new_std_io_error)?;
        self.io.flush().await.map_err(new_std_io_error)?;

        let response = self.read_response(Opcode::Get, opaque).await?;
        match response.header.vbucket_id_or_status {
            constants::OK_STATUS => Ok(Some(response.value)),
            constants::KEY_NOT_FOUND => Ok(None),
            status => Err(status_error(status, &response.value)),
        }
    }

    pub async fn set(&mut self, key: &str, val: &[u8], expiration: u32) -> Result<()> {
        self.ensure_healthy()?;

        let key_length = encode_key_length(key)?;
        let extras_length = 8u8;
        let total_body_length = encode_total_body_length(extras_length, key_length, val.len())?;

        let opaque = self.allocate_opaque();
        let request_header = PacketHeader {
            magic: Magic::Request as u8,
            opcode: Opcode::Set as u8,
            key_length,
            extras_length,
            total_body_length,
            opaque,
            ..Default::default()
        };
        let extras = StoreExtras {
            flags: 0,
            expiration,
        };

        self.poisoned = true;
        request_header
            .write(&mut self.io)
            .await
            .map_err(new_std_io_error)?;
        self.io
            .write_u32(extras.flags)
            .await
            .map_err(new_std_io_error)?;
        self.io
            .write_u32(extras.expiration)
            .await
            .map_err(new_std_io_error)?;
        self.io
            .write_all(key.as_bytes())
            .await
            .map_err(new_std_io_error)?;
        self.io.write_all(val).await.map_err(new_std_io_error)?;
        self.io.flush().await.map_err(new_std_io_error)?;

        let response = self.read_response(Opcode::Set, opaque).await?;
        if response.header.vbucket_id_or_status != constants::OK_STATUS {
            return Err(status_error(
                response.header.vbucket_id_or_status,
                &response.value,
            ));
        }
        Ok(())
    }

    pub async fn delete(&mut self, key: &str) -> Result<()> {
        self.ensure_healthy()?;

        let key_length = encode_key_length(key)?;
        let total_body_length = encode_total_body_length(0, key_length, 0)?;

        let opaque = self.allocate_opaque();
        let request_header = PacketHeader {
            magic: Magic::Request as u8,
            opcode: Opcode::Delete as u8,
            key_length,
            total_body_length,
            opaque,
            ..Default::default()
        };

        self.poisoned = true;
        request_header
            .write(&mut self.io)
            .await
            .map_err(new_std_io_error)?;
        self.io
            .write_all(key.as_bytes())
            .await
            .map_err(new_std_io_error)?;
        self.io.flush().await.map_err(new_std_io_error)?;

        let response = self.read_response(Opcode::Delete, opaque).await?;
        match response.header.vbucket_id_or_status {
            constants::OK_STATUS | constants::KEY_NOT_FOUND => Ok(()),
            status => Err(status_error(status, &response.value)),
        }
    }

    async fn read_response(
        &mut self,
        expected_opcode: Opcode,
        expected_opaque: u32,
    ) -> Result<Response> {
        let header = PacketHeader::read(&mut self.io)
            .await
            .map_err(new_std_io_error)?;

        if header.magic != Magic::Response as u8 {
            return Err(
                Error::new(ErrorKind::Unexpected, "unexpected response magic received")
                    .with_context("magic", format!("0x{:02x}", header.magic)),
            );
        }

        if header.opcode != expected_opcode as u8 {
            return Err(
                Error::new(ErrorKind::Unexpected, "unexpected response opcode received")
                    .with_context("expected", format!("0x{:02x}", expected_opcode as u8))
                    .with_context("actual", format!("0x{:02x}", header.opcode)),
            );
        }

        if header.opaque != expected_opaque {
            return Err(
                Error::new(ErrorKind::Unexpected, "unexpected response opaque received")
                    .with_context("expected", expected_opaque.to_string())
                    .with_context("actual", header.opaque.to_string()),
            );
        }

        let prefix_length = u32::from(header.extras_length) + u32::from(header.key_length);
        let Some(value_length) = header.total_body_length.checked_sub(prefix_length) else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "malformed response header: total_body_length is smaller than extras and key lengths",
            )
            .with_context("total_body_length", header.total_body_length.to_string())
            .with_context("extras_length", header.extras_length.to_string())
            .with_context("key_length", header.key_length.to_string()));
        };

        let is_get_ok =
            expected_opcode == Opcode::Get && header.vbucket_id_or_status == constants::OK_STATUS;
        if !is_get_ok && header.total_body_length > constants::MAX_CONTROL_BODY_LENGTH {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "memcached response body exceeds maximum allowed control/error size",
            )
            .with_context("status", format!("0x{:04x}", header.vbucket_id_or_status))
            .with_context("total_body_length", header.total_body_length.to_string()));
        }

        let value_len = usize::try_from(value_length).map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "memcached response value length exceeds platform usize",
            )
            .with_context("value_length", value_length.to_string())
        })?;

        let mut extras = vec![0x0; usize::from(header.extras_length)];
        self.io
            .read_exact(extras.as_mut_slice())
            .await
            .map_err(new_std_io_error)?;

        let mut key = vec![0x0; usize::from(header.key_length)];
        self.io
            .read_exact(key.as_mut_slice())
            .await
            .map_err(new_std_io_error)?;

        let mut value = vec![0x0; value_len];
        self.io
            .read_exact(value.as_mut_slice())
            .await
            .map_err(new_std_io_error)?;

        // Stream is now cleanly aligned at the next response frame boundary.
        self.poisoned = false;

        Ok(Response {
            header,
            _key: key,
            _extras: extras,
            value,
        })
    }
}

fn encode_key_length(key: &str) -> Result<u16> {
    u16::try_from(key.len()).map_err(|_| {
        Error::new(
            ErrorKind::Unsupported,
            "memcached key length exceeds binary protocol u16 limit",
        )
        .with_context("key_length", key.len().to_string())
    })
}

fn encode_total_body_length(
    extras_length: u8,
    key_length: u16,
    value_length: usize,
) -> Result<u32> {
    usize::from(extras_length)
        .checked_add(usize::from(key_length))
        .and_then(|n| n.checked_add(value_length))
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unsupported,
                "memcached packet body length exceeds binary protocol u32 limit",
            )
            .with_context("value_length", value_length.to_string())
        })
}

fn encode_auth_body_length(
    mechanism_length: usize,
    username_length: usize,
    password_length: usize,
) -> Result<u32> {
    mechanism_length
        .checked_add(1)
        .and_then(|n| n.checked_add(username_length))
        .and_then(|n| n.checked_add(1))
        .and_then(|n| n.checked_add(password_length))
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "memcached authentication payload exceeds binary protocol u32 limit",
            )
        })
}

fn status_error(status: u16, body: &[u8]) -> Error {
    let kind = match status {
        constants::AUTH_ERROR => ErrorKind::PermissionDenied,
        _ => ErrorKind::Unexpected,
    };
    let mut err = Error::new(kind, "memcached returned error status")
        .with_context("status", format!("0x{status:04x}"));
    if !body.is_empty() {
        err = err.with_context("message", String::from_utf8_lossy(body).into_owned());
    }
    err
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    async fn read_request_frame(stream: &mut TcpStream) -> (PacketHeader, Vec<u8>) {
        let mut header_buf = [0u8; HEADER_LENGTH];
        stream.read_exact(&mut header_buf).await.unwrap();
        let header = PacketHeader::from_bytes(header_buf);
        let mut body = vec![0u8; header.total_body_length as usize];
        stream.read_exact(&mut body).await.unwrap();
        (header, body)
    }

    async fn write_response_frame(
        stream: &mut TcpStream,
        magic: u8,
        opcode: u8,
        status: u16,
        opaque: u32,
        extras: &[u8],
        value: &[u8],
    ) {
        let header = PacketHeader {
            magic,
            opcode,
            extras_length: extras.len() as u8,
            vbucket_id_or_status: status,
            total_body_length: (extras.len() + value.len()) as u32,
            opaque,
            ..Default::default()
        };
        stream.write_all(&header.to_bytes()).await.unwrap();
        stream.write_all(extras).await.unwrap();
        stream.write_all(value).await.unwrap();
        stream.flush().await.unwrap();
    }

    async fn connect_pair() -> (Connection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (client, server) = tokio::join!(SocketStream::connect_tcp(&addr), listener.accept());
        (Connection::new(client.unwrap()), server.unwrap().0)
    }

    #[tokio::test]
    async fn server_error_drains_body_and_keeps_stream_synchronized() {
        let (mut conn, mut server) = connect_pair().await;

        let server_task = tokio::spawn(async move {
            let (set_hdr, _set_body) = read_request_frame(&mut server).await;
            assert_eq!(set_hdr.opcode, Opcode::Set as u8);
            assert_ne!(set_hdr.opaque, 0);
            write_response_frame(
                &mut server,
                Magic::Response as u8,
                Opcode::Set as u8,
                0x0003,
                set_hdr.opaque,
                &[],
                b"Object too large for cache",
            )
            .await;

            let (get_hdr, get_body) = read_request_frame(&mut server).await;
            assert_eq!(get_hdr.opcode, Opcode::Get as u8);
            assert_eq!(get_hdr.opaque, set_hdr.opaque + 1);
            assert_eq!(get_body, b"/next");
            write_response_frame(
                &mut server,
                Magic::Response as u8,
                Opcode::Get as u8,
                constants::OK_STATUS,
                get_hdr.opaque,
                &[0, 0, 0, 0],
                b"clean-value",
            )
            .await;
        });

        let err = conn.set("/big", b"payload", 0).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(format!("{err:?}").contains("Object too large for cache"));
        assert!(!conn.is_poisoned());

        let val = conn.get("/next").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"clean-value".as_slice()));
        assert!(!conn.is_poisoned());

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_malformed_response_headers_without_underflow_or_oom() {
        // Case 1: total_body_length < extras_length + key_length (previously underflowed/OOMed)
        {
            let (mut conn, mut server) = connect_pair().await;
            let server_task = tokio::spawn(async move {
                let (req, _) = read_request_frame(&mut server).await;
                let bad = PacketHeader {
                    magic: Magic::Response as u8,
                    opcode: Opcode::Get as u8,
                    key_length: 4,
                    extras_length: 4,
                    vbucket_id_or_status: constants::OK_STATUS,
                    total_body_length: 2,
                    opaque: req.opaque,
                    ..Default::default()
                };
                server.write_all(&bad.to_bytes()).await.unwrap();
                server.flush().await.unwrap();
            });

            let err = conn.get("/k").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(conn.is_poisoned());
            // Subsequent call on poisoned connection fails immediately without touching wire.
            let err2 = conn.get("/k").await.unwrap_err();
            assert_eq!(err2.kind(), ErrorKind::Unexpected);
            server_task.await.unwrap();
        }

        // Case 2: wrong magic
        {
            let (mut conn, mut server) = connect_pair().await;
            let server_task = tokio::spawn(async move {
                let (req, _) = read_request_frame(&mut server).await;
                write_response_frame(
                    &mut server,
                    Magic::Request as u8,
                    Opcode::Get as u8,
                    constants::OK_STATUS,
                    req.opaque,
                    &[],
                    b"v",
                )
                .await;
            });

            let err = conn.get("/k").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(conn.is_poisoned());
            server_task.await.unwrap();
        }

        // Case 3: wrong opcode
        {
            let (mut conn, mut server) = connect_pair().await;
            let server_task = tokio::spawn(async move {
                let (req, _) = read_request_frame(&mut server).await;
                write_response_frame(
                    &mut server,
                    Magic::Response as u8,
                    Opcode::Version as u8,
                    constants::OK_STATUS,
                    req.opaque,
                    &[],
                    b"1.6.29",
                )
                .await;
            });

            let err = conn.get("/k").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(conn.is_poisoned());
            server_task.await.unwrap();
        }

        // Case 4: wrong opaque
        {
            let (mut conn, mut server) = connect_pair().await;
            let server_task = tokio::spawn(async move {
                let (req, _) = read_request_frame(&mut server).await;
                write_response_frame(
                    &mut server,
                    Magic::Response as u8,
                    Opcode::Get as u8,
                    constants::OK_STATUS,
                    req.opaque + 1,
                    &[0, 0, 0, 0],
                    b"stale",
                )
                .await;
            });

            let err = conn.get("/k").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(conn.is_poisoned());
            server_task.await.unwrap();
        }

        // Case 5: oversized error response body is rejected without allocating/draining it
        {
            let (mut conn, mut server) = connect_pair().await;
            let server_task = tokio::spawn(async move {
                let (req, _) = read_request_frame(&mut server).await;
                let huge_err = PacketHeader {
                    magic: Magic::Response as u8,
                    opcode: Opcode::Set as u8,
                    vbucket_id_or_status: 0x0003,
                    total_body_length: constants::MAX_CONTROL_BODY_LENGTH + 1,
                    opaque: req.opaque,
                    ..Default::default()
                };
                server.write_all(&huge_err.to_bytes()).await.unwrap();
                server.flush().await.unwrap();
            });

            let err = conn.set("/k", b"v", 0).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(conn.is_poisoned());
            server_task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn key_not_found_is_rejected_for_set_version_and_auth_but_allowed_for_get_and_delete() {
        let (mut conn, mut server) = connect_pair().await;
        let server_task = tokio::spawn(async move {
            for expected_opcode in [
                Opcode::Get,
                Opcode::Delete,
                Opcode::Set,
                Opcode::Version,
                Opcode::StartAuth,
                Opcode::StartAuth,
            ] {
                let (req, _) = read_request_frame(&mut server).await;
                assert_eq!(req.opcode, expected_opcode as u8);
                let status = if expected_opcode == Opcode::StartAuth && req.opaque == 6 {
                    constants::AUTH_ERROR
                } else {
                    constants::KEY_NOT_FOUND
                };
                write_response_frame(
                    &mut server,
                    Magic::Response as u8,
                    req.opcode,
                    status,
                    req.opaque,
                    &[],
                    b"Not found",
                )
                .await;
            }
        });

        assert_eq!(conn.get("/missing").await.unwrap(), None);
        conn.delete("/missing").await.unwrap();

        let err = conn.set("/k", b"v", 0).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = conn.version().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = conn.auth("u", "p").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = conn.auth("u", "bad").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);

        assert!(!conn.is_poisoned());
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_key_is_rejected_without_writing_or_poisoning_socket() {
        let (mut conn, mut server) = connect_pair().await;

        let server_task = tokio::spawn(async move {
            // Only the final valid GET should ever reach the server.
            let (req, body) = read_request_frame(&mut server).await;
            assert_eq!(req.opcode, Opcode::Get as u8);
            assert_eq!(body, b"/ok");
            write_response_frame(
                &mut server,
                Magic::Response as u8,
                Opcode::Get as u8,
                constants::OK_STATUS,
                req.opaque,
                &[0, 0, 0, 0],
                b"alive",
            )
            .await;
        });

        let key = "a".repeat(usize::from(u16::MAX) + 1);
        assert_eq!(
            conn.get(&key).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            conn.set(&key, b"v", 0).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            conn.delete(&key).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert!(!conn.is_poisoned());

        // Connection must still be clean and usable without reconnect.
        let val = conn.get("/ok").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"alive".as_slice()));
        server_task.await.unwrap();
    }

    #[test]
    fn checked_body_length_rejects_u32_overflow() {
        let err = encode_total_body_length(8, 10, u32::MAX as usize).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        let err = encode_total_body_length(8, 10, (u32::MAX as usize) - 17).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert_eq!(
            encode_total_body_length(8, 10, (u32::MAX as usize) - 18).unwrap(),
            u32::MAX
        );

        let err = encode_auth_body_length(5, u32::MAX as usize, 0).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        let err = encode_auth_body_length(5, usize::MAX, 1).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }
}
