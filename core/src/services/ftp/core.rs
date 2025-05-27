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

use bb8::Pool;
use bb8::PooledConnection;
use bb8::RunError;
use futures_rustls::TlsConnector;
use raw::Operation;
use suppaftp::rustls::ClientConfig;
use suppaftp::types::FileType;
use suppaftp::AsyncRustlsConnector;
use suppaftp::AsyncRustlsFtpStream;
use suppaftp::FtpError;
use suppaftp::ImplAsyncFtpStream;
use suppaftp::Status;
use tokio::sync::OnceCell;

use super::err::parse_error;
use crate::raw::AccessorInfo;
use crate::*;

pub struct FtpCore {
    pub info: Arc<AccessorInfo>,
    pub manager: Manager,
    pub pool: OnceCell<Pool<Manager>>,
}

impl FtpCore {
    pub async fn ftp_connect(&self, _: Operation) -> Result<PooledConnection<'static, Manager>> {
        let pool = self
            .pool
            .get_or_try_init(|| async {
                bb8::Pool::builder()
                    .max_size(64)
                    .build(self.manager.clone())
                    .await
            })
            .await
            .map_err(parse_error)?;

        pool.get_owned().await.map_err(|err| match err {
            RunError::User(err) => parse_error(err),
            RunError::TimedOut => {
                Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary()
            }
        })
    }
}

#[derive(Clone)]
pub struct Manager {
    pub endpoint: String,
    pub root: String,
    pub user: String,
    pub password: String,
    pub enable_secure: bool,
}

impl bb8::ManageConnection for Manager {
    type Connection = AsyncRustlsFtpStream;
    type Error = FtpError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let stream = ImplAsyncFtpStream::connect(&self.endpoint).await?;
        // switch to secure mode if ssl/tls is on.
        let mut ftp_stream = if self.enable_secure {
            let mut root_store = suppaftp::rustls::RootCertStore::empty();
            for cert in
                rustls_native_certs::load_native_certs().expect("could not load platform certs")
            {
                root_store.add(cert).unwrap();
            }

            let cfg = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            stream
                .into_secure(
                    AsyncRustlsConnector::from(TlsConnector::from(Arc::new(cfg))),
                    &self.endpoint,
                )
                .await?
        } else {
            stream
        };

        // login if needed
        if !self.user.is_empty() {
            ftp_stream.login(&self.user, &self.password).await?;
        }

        // change to the root path
        match ftp_stream.cwd(&self.root).await {
            Err(FtpError::UnexpectedResponse(e)) if e.status == Status::FileUnavailable => {
                ftp_stream.mkdir(&self.root).await?;
                // Then change to root path
                ftp_stream.cwd(&self.root).await?;
            }
            // Other errors, return.
            Err(e) => return Err(e),
            // Do nothing if success.
            Ok(_) => (),
        }

        ftp_stream.transfer_type(FileType::Binary).await?;

        Ok(ftp_stream)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.noop().await
    }

    /// Don't allow reuse conn.
    ///
    /// We need to investigate why reuse conn will cause error.
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        true
    }
}
