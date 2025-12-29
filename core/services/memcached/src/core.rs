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

use fastpool::ManageObject;
use fastpool::ObjectStatus;
use fastpool::bounded;
use opendal_core::raw::*;
use opendal_core::*;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};

use super::binary;

#[derive(Debug)]
pub enum SocketStream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl SocketStream {
    pub async fn connect_any(addr_str: &str) -> io::Result<Self> {
        if let Ok(socket_addr) = addr_str.parse::<SocketAddr>() {
            let stream = TcpStream::connect(socket_addr).await?;
            Ok(SocketStream::Tcp(stream))
        } else {
            let stream = UnixStream::connect(addr_str).await?;
            Ok(SocketStream::Unix(stream))
        }
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
            SocketStream::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            SocketStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            SocketStream::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            SocketStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            SocketStream::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// A connection manager for `memcache_async::ascii::Protocol`.
#[derive(Clone)]
struct MemcacheConnectionManager {
    address: String,
    username: Option<String>,
    password: Option<String>,
}

impl MemcacheConnectionManager {
    fn new(address: &str, username: Option<String>, password: Option<String>) -> Self {
        Self {
            address: address.to_string(),
            username,
            password,
        }
    }
}

impl ManageObject for MemcacheConnectionManager {
    type Object = binary::Connection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        let conn = SocketStream::connect_any(&self.address)
            .await
            .map_err(new_std_io_error)?;

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
        match o.version().await {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemcachedCore {
    default_ttl: Option<Duration>,
    conn: Arc<bounded::Pool<MemcacheConnectionManager>>,
}

impl MemcachedCore {
    pub fn new(
        endpoint: String,
        username: Option<String>,
        password: Option<String>,
        default_ttl: Option<Duration>,
        connection_pool_max_size: Option<usize>,
    ) -> Self {
        let conn = bounded::Pool::new(
            bounded::PoolConfig::new(connection_pool_max_size.unwrap_or(10)),
            MemcacheConnectionManager::new(endpoint.as_str(), username, password),
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

    pub async fn set(&self, key: &str, value: Buffer) -> Result<()> {
        let mut conn = self.conn().await?;

        conn.set(
            &percent_encode_path(key),
            &value.to_vec(),
            // Set expiration to 0 if ttl not set.
            self.default_ttl
                .map(|v| v.as_secs() as u32)
                .unwrap_or_default(),
        )
        .await
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.conn().await?;

        conn.delete(&percent_encode_path(key)).await
    }
}
