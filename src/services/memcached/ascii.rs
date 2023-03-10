// Copyright 2017 vavrusa <marek@vavrusa.com>
//
// Licensed under the MIT License (see MIT-ascii.txt);

use core::fmt::Display;
use std::io::Error;
use std::io::ErrorKind;
use std::marker::Unpin;

use futures::io::AsyncBufReadExt;
use futures::io::AsyncRead;
use futures::io::AsyncReadExt;
use futures::io::AsyncWrite;
use futures::io::AsyncWriteExt;
use futures::io::BufReader;

/// Memcache ASCII protocol implementation.
pub struct Protocol<S> {
    io: BufReader<S>,
    buf: Vec<u8>,
}

impl<S> Protocol<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Creates the ASCII protocol on a stream.
    pub fn new(io: S) -> Self {
        Self {
            io: BufReader::new(io),
            buf: Vec::new(),
        }
    }

    /// Returns the value for given key as bytes. If the value doesn't exist, [`ErrorKind::NotFound`] is returned.
    pub async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Vec<u8>, Error> {
        // Send command
        let writer = self.io.get_mut();
        writer
            .write_all(&[b"get ", key.as_ref(), b"\r\n"].concat())
            .await?;
        writer.flush().await?;

        // Read response header
        let header = self.read_line().await?;
        let header = std::str::from_utf8(header).map_err(|_| ErrorKind::InvalidData)?;

        // Check response header and parse value length
        if header.contains("ERROR") {
            return Err(Error::new(ErrorKind::Other, header));
        } else if header.starts_with("END") {
            return Err(ErrorKind::NotFound.into());
        }

        // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        let length: usize = header
            .split(' ')
            .nth(3)
            .and_then(|len| len.trim_end().parse().ok())
            .ok_or(ErrorKind::InvalidData)?;

        // Read value
        let mut buffer: Vec<u8> = vec![0; length];
        self.io.read_exact(&mut buffer).await?;

        // Read the trailing header
        self.read_line().await?; // \r\n
        self.read_line().await?; // END\r\n

        Ok(buffer)
    }

    /// Set key to given value and don't wait for response.
    pub async fn set<K: Display>(
        &mut self,
        key: K,
        val: &[u8],
        expiration: u32,
    ) -> Result<(), Error> {
        let header = format!("set {} 0 {} {}\r\n", key, expiration, val.len());
        self.io.write_all(header.as_bytes()).await?;
        self.io.write_all(val).await?;
        self.io.write_all(b"\r\n").await?;
        self.io.flush().await?;

        // Read response header
        let header = self.read_line().await?;
        let header = std::str::from_utf8(header).map_err(|_| ErrorKind::InvalidData)?;
        // Check response header and make sure we got a `STORED`
        if header.contains("STORED") {
            return Ok(());
        } else if header.contains("ERROR") {
            return Err(Error::new(ErrorKind::Other, header));
        }
        Ok(())
    }

    /// Delete a key and don't wait for response.
    pub async fn delete<K: Display>(&mut self, key: K) -> Result<(), Error> {
        let header = format!("delete {}\r\n", key);
        self.io.write_all(header.as_bytes()).await?;
        self.io.flush().await?;

        // Read response header
        let header = self.read_line().await?;
        let header = std::str::from_utf8(header).map_err(|_| ErrorKind::InvalidData)?;
        // Check response header and parse value length
        if header.contains("NOT_FOUND") {
            return Ok(());
        } else if header.starts_with("END") {
            return Err(ErrorKind::NotFound.into());
        } else if header.contains("ERROR") || !header.contains("DELETED") {
            return Err(Error::new(ErrorKind::Other, header));
        }
        Ok(())
    }

    /// Return the version of the remote server.
    pub async fn version(&mut self) -> Result<String, Error> {
        self.io.write_all(b"version\r\n").await?;
        self.io.flush().await?;

        // Read response header
        let header = {
            let buf = self.read_line().await?;
            std::str::from_utf8(buf).map_err(|_| Error::from(ErrorKind::InvalidData))?
        };

        if !header.starts_with("VERSION") {
            return Err(Error::new(ErrorKind::Other, header));
        }
        let version = header.trim_start_matches("VERSION ").trim_end();
        Ok(version.to_string())
    }

    async fn read_line(&mut self) -> Result<&[u8], Error> {
        let Self { io, buf } = self;
        buf.clear();
        io.read_until(b'\n', buf).await?;
        if buf.last().copied() != Some(b'\n') {
            return Err(ErrorKind::UnexpectedEof.into());
        }
        Ok(&buf[..])
    }
}
