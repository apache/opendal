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

use fastpool::{ManageObject, ObjectStatus, bounded};
use futures_rustls::TlsConnector;
use suppaftp::AsyncRustlsConnector;
use suppaftp::AsyncRustlsFtpStream;
use suppaftp::FtpError;
use suppaftp::ImplAsyncFtpStream;
use suppaftp::Status;
use suppaftp::rustls::ClientConfig;
use suppaftp::types::FileType;

use super::err::format_ftp_error;
use opendal_core::raw::*;
use opendal_core::*;

pub struct FtpCore {
    info: Arc<AccessorInfo>,
    pool: Arc<bounded::Pool<Manager>>,
}

impl FtpCore {
    pub fn new(info: Arc<AccessorInfo>, manager: Manager) -> Self {
        let pool = bounded::Pool::new(bounded::PoolConfig::new(64), manager);
        Self { info, pool }
    }

    pub fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    pub async fn ftp_connect(&self, _: Operation) -> Result<bounded::Object<Manager>> {
        let fut = self.pool.get();

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                Err(Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary())
            }
            result = fut => match result {
                Ok(conn) => Ok(conn),
                Err(err) => Err(format_ftp_error(err)),
            }
        }
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

impl ManageObject for Manager {
    type Object = AsyncRustlsFtpStream;
    type Error = FtpError;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
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

    async fn is_recyclable(
        &self,
        o: &mut Self::Object,
        _: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        o.noop().await
    }
}
