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

use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpStream;

use crate::raw::*;
use crate::*;

pub struct Connection {
    io: BufReader<TcpStream>,
    buf: Vec<u8>,
}

impl Connection {
    pub fn new(io: TcpStream) -> Self {
        Self {
            io: BufReader::new(io),
            buf: Vec::new(),
        }
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        // Send command
        let writer = self.io.get_mut();
        writer
            .write_all(&[b"get ", key.as_bytes(), b"\r\n"].concat())
            .await
            .map_err(new_std_io_error)?;
        writer.flush().await.map_err(new_std_io_error)?;

        // Read response header
        let header = self.read_header().await?;

        // Check response header and parse value length
        if header.contains("ERROR") {
            return Err(
                Error::new(ErrorKind::Unexpected, "unexpected data received")
                    .with_context("message", header),
            );
        } else if header.starts_with("END") {
            return Ok(None);
        }

        // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        let length: usize = header
            .split(' ')
            .nth(3)
            .and_then(|len| len.trim_end().parse().ok())
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "invalid data received"))?;

        // Read value
        let mut buffer: Vec<u8> = vec![0; length];
        self.io
            .read_exact(&mut buffer)
            .await
            .map_err(new_std_io_error)?;

        // Read the trailing header
        self.read_line().await?; // \r\n
        self.read_line().await?; // END\r\n

        Ok(Some(buffer))
    }

    pub async fn set(&mut self, key: &str, val: &[u8], expiration: u32) -> Result<()> {
        let header = format!("set {} 0 {} {}\r\n", key, expiration, val.len());
        self.io
            .write_all(header.as_bytes())
            .await
            .map_err(new_std_io_error)?;
        self.io.write_all(val).await.map_err(new_std_io_error)?;
        self.io.write_all(b"\r\n").await.map_err(new_std_io_error)?;
        self.io.flush().await.map_err(new_std_io_error)?;

        // Read response header
        let header = self.read_header().await?;

        // Check response header and make sure we got a `STORED`
        if header.contains("STORED") {
            return Ok(());
        } else if header.contains("ERROR") {
            return Err(
                Error::new(ErrorKind::Unexpected, "unexpected data received")
                    .with_context("message", header),
            );
        }
        Ok(())
    }

    pub async fn delete(&mut self, key: &str) -> Result<()> {
        let header = format!("delete {}\r\n", key);
        self.io
            .write_all(header.as_bytes())
            .await
            .map_err(new_std_io_error)?;
        self.io.flush().await.map_err(new_std_io_error)?;

        // Read response header
        let header = self.read_header().await?;

        // Check response header and parse value length
        if header.contains("NOT_FOUND") || header.starts_with("END") {
            return Ok(());
        } else if header.contains("ERROR") || !header.contains("DELETED") {
            return Err(
                Error::new(ErrorKind::Unexpected, "unexpected data received")
                    .with_context("message", header),
            );
        }
        Ok(())
    }

    pub async fn version(&mut self) -> Result<String> {
        self.io
            .write_all(b"version\r\n")
            .await
            .map_err(new_std_io_error)?;
        self.io.flush().await.map_err(new_std_io_error)?;

        // Read response header
        let header = self.read_header().await?;

        if !header.starts_with("VERSION") {
            return Err(
                Error::new(ErrorKind::Unexpected, "unexpected data received")
                    .with_context("message", header),
            );
        }
        let version = header.trim_start_matches("VERSION ").trim_end();
        Ok(version.to_string())
    }

    async fn read_line(&mut self) -> Result<&[u8]> {
        let Self { io, buf } = self;
        buf.clear();
        io.read_until(b'\n', buf).await.map_err(new_std_io_error)?;
        if buf.last().copied() != Some(b'\n') {
            return Err(Error::new(
                ErrorKind::ContentIncomplete,
                "unexpected eof, the response must be incomplete",
            ));
        }
        Ok(&buf[..])
    }

    async fn read_header(&mut self) -> Result<&str> {
        let header = self.read_line().await?;
        let header = std::str::from_utf8(header).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "invalid data received").set_source(err)
        })?;

        Ok(header)
    }
}
