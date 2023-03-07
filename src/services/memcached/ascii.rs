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
        let header = format!("set {} 0 {} {} noreply\r\n", key, expiration, val.len());
        self.io.write_all(header.as_bytes()).await?;
        self.io.write_all(val).await?;
        self.io.write_all(b"\r\n").await?;
        self.io.flush().await?;
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
        if  header.contains("NOT_FOUND"){
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::io::Error;
    use std::io::ErrorKind;
    use std::io::Read;
    use std::io::Write;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;

    use futures::executor::block_on;
    use futures::io::AsyncRead;
    use futures::io::AsyncWrite;

    struct Cache {
        r: Cursor<Vec<u8>>,
        w: Cursor<Vec<u8>>,
    }

    impl Cache {
        fn new() -> Self {
            Cache {
                r: Cursor::new(Vec::new()),
                w: Cursor::new(Vec::new()),
            }
        }
    }

    impl AsyncRead for Cache {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context,
            buf: &mut [u8],
        ) -> Poll<Result<usize, Error>> {
            Poll::Ready(self.get_mut().r.read(buf))
        }
    }

    impl AsyncWrite for Cache {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context,
            buf: &[u8],
        ) -> Poll<Result<usize, Error>> {
            Poll::Ready(self.get_mut().w.write(buf))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Error>> {
            Poll::Ready(self.get_mut().w.flush())
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn test_ascii_get() {
        let mut cache = Cache::new();
        cache
            .r
            .get_mut()
            .extend_from_slice(b"VALUE foo 0 3\r\nbar\r\nEND\r\n");
        let mut ascii = super::Protocol::new(&mut cache);
        assert_eq!(block_on(ascii.get(&"foo")).unwrap(), b"bar");
        assert_eq!(cache.w.get_ref(), b"get foo\r\n");
    }

    #[test]
    fn test_ascii_get2() {
        let mut cache = Cache::new();
        cache
            .r
            .get_mut()
            .extend_from_slice(b"VALUE foo 0 3\r\nbar\r\nEND\r\nVALUE bar 0 3\r\nbaz\r\nEND\r\n");
        let mut ascii = super::Protocol::new(&mut cache);
        assert_eq!(block_on(ascii.get(&"foo")).unwrap(), b"bar");
        assert_eq!(block_on(ascii.get(&"bar")).unwrap(), b"baz");
    }

    #[test]
    fn test_ascii_get_cas() {
        let mut cache = Cache::new();
        cache
            .r
            .get_mut()
            .extend_from_slice(b"VALUE foo 0 3 99999\r\nbar\r\nEND\r\n");
        let mut ascii = super::Protocol::new(&mut cache);
        assert_eq!(block_on(ascii.get(&"foo")).unwrap(), b"bar");
        assert_eq!(cache.w.get_ref(), b"get foo\r\n");
    }

    #[test]
    fn test_ascii_get_empty() {
        let mut cache = Cache::new();
        cache.r.get_mut().extend_from_slice(b"END\r\n");
        let mut ascii = super::Protocol::new(&mut cache);
        assert_eq!(
            block_on(ascii.get(&"foo")).unwrap_err().kind(),
            ErrorKind::NotFound
        );
        assert_eq!(cache.w.get_ref(), b"get foo\r\n");
    }

    #[test]
    fn test_ascii_get_eof_error() {
        let mut cache = Cache::new();
        cache.r.get_mut().extend_from_slice(b"EN");
        let mut ascii = super::Protocol::new(&mut cache);
        assert_eq!(
            block_on(ascii.get(&"foo")).unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );
        assert_eq!(cache.w.get_ref(), b"get foo\r\n");
    }

    #[test]
    fn test_ascii_set() {
        let (key, val, ttl) = ("foo", "bar", 5);
        let mut cache = Cache::new();
        let mut ascii = super::Protocol::new(&mut cache);
        block_on(ascii.set(&key, val.as_bytes(), ttl)).unwrap();
        assert_eq!(
            cache.w.get_ref(),
            &format!("set {} 0 {} {} noreply\r\n{}\r\n", key, ttl, val.len(), val)
                .as_bytes()
                .to_vec()
        );
    }

    #[test]
    fn test_ascii_version() {
        let mut cache = Cache::new();
        cache.r.get_mut().extend_from_slice(b"VERSION 1.6.6\r\n");
        let mut ascii = super::Protocol::new(&mut cache);
        assert_eq!(block_on(ascii.version()).unwrap(), "1.6.6");
        assert_eq!(cache.w.get_ref(), b"version\r\n");
    }
}
