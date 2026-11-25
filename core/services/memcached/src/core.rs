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

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::bounded;
use opendal_core::raw::*;
use opendal_core::*;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, pki_types::ServerName};

use super::binary;

#[derive(Debug)]
pub enum SocketStream {
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl SocketStream {
    pub async fn connect_tcp(addr_str: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr_str).await?;
        Ok(SocketStream::Tcp(stream))
    }

    #[cfg(unix)]
    pub async fn connect_unix(path: &str) -> io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(SocketStream::Unix(stream))
    }
}

impl AsyncRead for SocketStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            SocketStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            SocketStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            SocketStream::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for SocketStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            SocketStream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            SocketStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            SocketStream::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            SocketStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            SocketStream::Tls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            SocketStream::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            SocketStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            SocketStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            SocketStream::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Endpoint for memcached connection.
#[derive(Clone, Debug)]
pub enum Endpoint {
    Tcp(String), // host:port
    Tls {
        address: String,
        server_name: ServerName<'static>,
        config: Arc<ClientConfig>,
    },
    #[cfg(unix)]
    Unix(String), // socket path
}

impl Endpoint {
    pub(super) fn tls(address: String, host: &str) -> Result<Self> {
        let server_name =
            ServerName::try_from(host.trim_matches(['[', ']']).to_owned()).map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "invalid TLS server name").set_source(err)
            })?;
        let native = rustls_native_certs::load_native_certs();
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(native.certs);
        if roots.is_empty() {
            let mut err = Error::new(
                ErrorKind::ConfigInvalid,
                "no usable native root certificates were loaded",
            );
            if let Some(source) = native.errors.into_iter().next() {
                err = err.set_source(source);
            }
            return Err(err);
        }
        let config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|err| {
                    Error::new(ErrorKind::ConfigInvalid, "TLS protocols are unavailable")
                        .set_source(err)
                })?
                .with_root_certificates(roots)
                .with_no_client_auth();
        Ok(Self::Tls {
            address,
            server_name,
            config: Arc::new(config),
        })
    }
}

/// A connection manager for `memcache_async::ascii::Protocol`.
#[derive(Clone)]
struct MemcacheConnectionManager {
    endpoint: Endpoint,
    username: Option<String>,
    password: Option<String>,
}

impl MemcacheConnectionManager {
    fn new(endpoint: Endpoint, username: Option<String>, password: Option<String>) -> Self {
        Self {
            endpoint,
            username,
            password,
        }
    }
}

impl ManageObject for MemcacheConnectionManager {
    type Object = binary::Connection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        let conn = match &self.endpoint {
            Endpoint::Tcp(addr) => SocketStream::connect_tcp(addr)
                .await
                .map_err(new_std_io_error)?,
            Endpoint::Tls {
                address,
                server_name,
                config,
            } => {
                let stream = TcpStream::connect(address)
                    .await
                    .map_err(new_std_io_error)?;
                let stream = TlsConnector::from(Arc::clone(config))
                    .connect(server_name.clone(), stream)
                    .await
                    .map_err(new_std_io_error)?;
                SocketStream::Tls(Box::new(stream))
            }
            #[cfg(unix)]
            Endpoint::Unix(path) => SocketStream::connect_unix(path)
                .await
                .map_err(new_std_io_error)?,
        };

        let mut conn = binary::Connection::new(conn);

        if let (Some(username), Some(password)) = (self.username.as_ref(), self.password.as_ref()) {
            conn.auth(username, password).await?;
        }
        Ok(conn)
    }

    async fn is_recyclable(
        &self,
        o: &mut Self::Object,
        _: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        if o.is_poisoned() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "memcached connection is poisoned",
            ));
        }
        o.version().await.map(|_| ())
    }
}

#[derive(Clone, Debug)]
pub struct MemcachedCore {
    default_ttl: Option<Duration>,
    conn: Arc<bounded::Pool<MemcacheConnectionManager>>,
}

/// memcached reads an expiration larger than 30 days as an absolute Unix
/// timestamp rather than as an offset from now, so a longer TTL sent verbatim
/// lands in 1970 and the server stores the item as already expired.
///
/// See the `Expiration times` section of memcached's protocol.txt.
pub(super) const MAX_RELATIVE_EXPIRATION_SECS: u64 = 60 * 60 * 24 * 30;

impl MemcachedCore {
    pub fn new(
        endpoint: Endpoint,
        username: Option<String>,
        password: Option<String>,
        default_ttl: Option<Duration>,
        connection_pool_max_size: Option<usize>,
    ) -> Self {
        let conn = bounded::Pool::new(
            bounded::PoolConfig::new(connection_pool_max_size.unwrap_or(10)),
            MemcacheConnectionManager::new(endpoint, username, password),
        );

        Self { default_ttl, conn }
    }

    async fn conn(&self) -> Result<bounded::Object<MemcacheConnectionManager>> {
        let fut = self.conn.get();

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                Err(Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary())
            }
            result = fut => match result {
                Ok(conn) => Ok(conn),
                Err(err) => Err(err),
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let mut conn = self.conn().await?;
        let result = conn.get(&percent_encode_path(key)).await?;
        Ok(result.map(Buffer::from))
    }

    pub async fn set(&self, key: &str, value: Buffer, expires: Option<Duration>) -> Result<()> {
        let expiration = resolve_expiration(expires, self.default_ttl)?;
        let mut conn = self.conn().await?;
        conn.set(&percent_encode_path(key), &value.to_vec(), expiration)
            .await
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.conn().await?;

        conn.delete(&percent_encode_path(key)).await
    }
}

fn resolve_expiration(expires: Option<Duration>, default_ttl: Option<Duration>) -> Result<u32> {
    if let Some(expires) = expires {
        return encode_expiration(expires);
    }

    match default_ttl {
        Some(ttl) if !ttl.is_zero() => encode_expiration(ttl),
        _ => Ok(0),
    }
}

fn encode_expiration(expires: Duration) -> Result<u32> {
    if expires.is_zero() {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            "expires must be positive",
        ));
    }
    let seconds = expires.as_nanos().div_ceil(1_000_000_000);
    let expiration = if seconds <= u128::from(MAX_RELATIVE_EXPIRATION_SECS) {
        seconds
    } else {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "system clock is before the Unix epoch",
                )
                .set_source(err)
            })?;
        u128::from(now.as_secs()) + seconds
    };
    u32::try_from(expiration).map_err(|err| {
        Error::new(
            ErrorKind::ConfigInvalid,
            "expires is too large for Memcached",
        )
        .set_source(err)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn resolve_memcached_expiration() {
        assert_eq!(resolve_expiration(None, None).unwrap(), 0);
        assert_eq!(resolve_expiration(None, Some(Duration::ZERO)).unwrap(), 0);

        for ttl in [
            Duration::from_nanos(1),
            Duration::from_millis(1),
            Duration::from_millis(999),
        ] {
            assert_eq!(resolve_expiration(None, Some(ttl)).unwrap(), 1);
        }

        assert_eq!(
            resolve_expiration(None, Some(Duration::from_secs(1))).unwrap(),
            1
        );
        assert_eq!(
            resolve_expiration(None, Some(Duration::from_secs(60))).unwrap(),
            60
        );
        assert_eq!(
            resolve_expiration(None, Some(Duration::from_millis(1500))).unwrap(),
            2
        );
        assert_eq!(
            resolve_expiration(Some(Duration::from_secs(60)), Some(Duration::from_nanos(1)))
                .unwrap(),
            60
        );

        let err =
            resolve_expiration(Some(Duration::ZERO), Some(Duration::from_secs(60))).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn encode_memcached_expiration() {
        let thirty_days = Duration::from_secs(30 * 24 * 60 * 60);
        assert_eq!(encode_expiration(Duration::from_millis(1)).unwrap(), 1);
        assert_eq!(encode_expiration(thirty_days).unwrap(), 2_592_000);

        let now = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        };
        let before = now();
        let deadline = encode_expiration(thirty_days + Duration::from_nanos(1)).unwrap();
        let after = now();
        assert!((before + 2_592_001..=after + 2_592_001).contains(&u64::from(deadline)));

        let err = encode_expiration(Duration::MAX).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(!err.is_temporary());
    }

    // regression test for connecting to a webdav server.
    // Because setting up a dedicated behavior test is expensive, we choose to test `SocketStream::connect_tcp` instead.
    // In the future, we could set up a webdav server to test TCP connection properly.
    #[tokio::test]
    async fn connect_tcp_socket() -> std::io::Result<()> {
        for addr in &["127.0.0.1:11211", "localhost:11211", "[::1]:11211"] {
            let listener = TcpListener::bind(addr).await?;
            let addr = listener.local_addr()?.to_string();

            let accepted = tokio::spawn(async move {
                let _ = listener.accept().await?;
                Ok::<_, std::io::Error>(())
            });

            let _stream = SocketStream::connect_tcp(&addr).await?;
            accepted.await.unwrap()?;
        }

        Ok(())
    }

    // regression test for connecting to a webdav server.
    // Because setting up a dedicated behavior test is expensive, we choose to test `SocketStream::connect_unix` instead.
    // In the future, we could set up a webdav server to test UNIX socket connection properly.
    #[cfg(unix)]
    #[tokio::test]
    async fn connect_unix_socket() -> std::io::Result<()> {
        use std::time::{SystemTime, UNIX_EPOCH};
        use tokio::net::UnixListener;

        let path = std::env::temp_dir().join(format!(
            "opendal-memcached-{}.sock",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let listener = UnixListener::bind(&path)?;

        let accepted = tokio::spawn(async move {
            let _ = listener.accept().await?;
            Ok::<_, std::io::Error>(())
        });

        let _stream = SocketStream::connect_unix(path.to_str().unwrap()).await?;
        accepted.await.unwrap()?;

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    async fn read_binary_request(
        stream: &mut TcpStream,
    ) -> std::io::Result<(binary::PacketHeader, Vec<u8>)> {
        use tokio::io::AsyncReadExt;

        let mut header_buf = [0u8; 24];
        stream.read_exact(&mut header_buf).await?;
        let header = binary::PacketHeader::from_bytes(header_buf);
        let mut body = vec![0u8; header.total_body_length as usize];
        stream.read_exact(&mut body).await?;
        Ok((header, body))
    }

    async fn write_binary_response(
        stream: &mut TcpStream,
        opcode: u8,
        status: u16,
        opaque: u32,
        extras: &[u8],
        value: &[u8],
    ) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;

        let header = binary::PacketHeader {
            magic: binary::Magic::Response as u8,
            opcode,
            extras_length: extras.len() as u8,
            vbucket_id_or_status: status,
            total_body_length: (extras.len() + value.len()) as u32,
            opaque,
            ..Default::default()
        };
        stream.write_all(&header.to_bytes()).await?;
        stream.write_all(extras).await?;
        stream.write_all(value).await?;
        stream.flush().await
    }

    #[tokio::test]
    async fn cancelled_pooled_request_discards_socket_and_prevents_cross_key_leak() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::oneshot;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepted_conns = Arc::new(AtomicUsize::new(0));
        let accepted_conns_server = Arc::clone(&accepted_conns);

        let (req1_received_tx, req1_received_rx) = oneshot::channel::<()>();
        let (release_reply1_tx, release_reply1_rx) = oneshot::channel::<()>();
        let (reply1_written_tx, reply1_written_rx) = oneshot::channel::<()>();

        let server_task = tokio::spawn(async move {
            // Connection #1: receives GET /foo, waits until cancelled, then writes stale reply.
            let (mut conn1, _) = listener.accept().await.unwrap();
            accepted_conns_server.fetch_add(1, Ordering::SeqCst);
            let (req1, body1) = read_binary_request(&mut conn1).await.unwrap();
            assert_eq!(req1.opcode, binary::Opcode::Get as u8);
            assert_eq!(body1, b"/foo");
            let _ = req1_received_tx.send(());
            let _ = release_reply1_rx.await;
            // Best-effort write: the client already dropped the future, so the stale reply
            // lands in the kernel socket buffer while conn1 sits idle in the pool.
            let _ = write_binary_response(
                &mut conn1,
                binary::Opcode::Get as u8,
                binary::constants::OK_STATUS,
                req1.opaque,
                &[0, 0, 0, 0],
                b"secret-foo-value",
            )
            .await;
            let _ = reply1_written_tx.send(());

            // Connection #2: pool must discard poisoned conn1 and open a fresh connection.
            let (mut conn2, _) = listener.accept().await.unwrap();
            accepted_conns_server.fetch_add(1, Ordering::SeqCst);
            let (req2, body2) = read_binary_request(&mut conn2).await.unwrap();
            assert_eq!(req2.opcode, binary::Opcode::Get as u8);
            assert_eq!(body2, b"/bar");
            write_binary_response(
                &mut conn2,
                binary::Opcode::Get as u8,
                binary::constants::OK_STATUS,
                req2.opaque,
                &[0, 0, 0, 0],
                b"value-of-bar",
            )
            .await
            .unwrap();
        });

        let core = MemcachedCore::new(Endpoint::Tcp(addr), None, None, None, Some(1));

        // Start GET /foo and deterministically cancel it mid-flight once the request is on the wire.
        let mut req1_received_rx = req1_received_rx;
        tokio::select! {
            _ = core.get("/foo") => panic!("first request should have been cancelled before replying"),
            res = &mut req1_received_rx => res.unwrap(),
        }

        // Let the server write the delayed /foo reply into conn1's socket buffer.
        release_reply1_tx.send(()).unwrap();
        reply1_written_rx.await.unwrap();

        // Next caller must never observe VERSION bytes or stale /foo data.
        let got = core
            .get("/bar")
            .await
            .unwrap()
            .expect("expected /bar value");
        assert_eq!(got.to_bytes().as_ref(), b"value-of-bar");
        assert_eq!(accepted_conns.load(Ordering::SeqCst), 2);

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn pooled_connection_recovers_after_server_error_with_body() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepted_conns = Arc::new(AtomicUsize::new(0));
        let accepted_conns_server = Arc::clone(&accepted_conns);

        let server_task = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            accepted_conns_server.fetch_add(1, Ordering::SeqCst);

            // 1. SET /big -> 0x0003 Value too large with ASCII error body.
            let (set_req, _) = read_binary_request(&mut conn).await.unwrap();
            assert_eq!(set_req.opcode, binary::Opcode::Set as u8);
            write_binary_response(
                &mut conn,
                binary::Opcode::Set as u8,
                0x0003,
                set_req.opaque,
                &[],
                b"Object too large for cache",
            )
            .await
            .unwrap();

            // 2. Pool recycles the same connection via VERSION health check.
            let (ver_req, _) = read_binary_request(&mut conn).await.unwrap();
            assert_eq!(ver_req.opcode, binary::Opcode::Version as u8);
            write_binary_response(
                &mut conn,
                binary::Opcode::Version as u8,
                binary::constants::OK_STATUS,
                ver_req.opaque,
                &[],
                b"1.6.29",
            )
            .await
            .unwrap();

            // 3. Subsequent GET /ok on the same connection returns clean value.
            let (get_req, get_body) = read_binary_request(&mut conn).await.unwrap();
            assert_eq!(get_req.opcode, binary::Opcode::Get as u8);
            assert_eq!(get_body, b"/ok");
            write_binary_response(
                &mut conn,
                binary::Opcode::Get as u8,
                binary::constants::OK_STATUS,
                get_req.opaque,
                &[0, 0, 0, 0],
                b"ok-value",
            )
            .await
            .unwrap();
        });

        let core = MemcachedCore::new(Endpoint::Tcp(addr), None, None, None, Some(1));

        let err = core
            .set("/big", Buffer::from("oversized"), None)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let got = core.get("/ok").await.unwrap().expect("expected /ok value");
        assert_eq!(got.to_bytes().as_ref(), b"ok-value");
        assert_eq!(accepted_conns.load(Ordering::SeqCst), 1);

        server_task.await.unwrap();
    }
}
