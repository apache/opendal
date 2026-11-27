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
use std::time::Duration;

use asyncband::pool::{ManageObject, ObjectStatus, bounded};
use log::warn;
use suppaftp::FtpError;
use suppaftp::Status;
use suppaftp::tokio::AsyncRustlsConnector;
use suppaftp::tokio::AsyncRustlsFtpStream;
use suppaftp::tokio::ImplAsyncFtpStream;
use suppaftp::tokio_rustls::TlsConnector;
use suppaftp::tokio_rustls::rustls::ClientConfig;
use suppaftp::tokio_rustls::rustls::RootCertStore;
use suppaftp::tokio_rustls::rustls::crypto::{CryptoProvider, ring};
use suppaftp::tokio_rustls::rustls::pki_types::CertificateDer;
use suppaftp::types::{FileType, Response};

use opendal_core::raw::*;
use opendal_core::*;

pub(crate) fn build_tls_config(
    native: rustls_native_certs::CertificateResult,
) -> Result<Arc<ClientConfig>> {
    build_tls_config_from_parts(native.certs, native.errors)
}

fn build_tls_config_from_parts(
    certs: Vec<CertificateDer<'static>>,
    errors: Vec<rustls_native_certs::Error>,
) -> Result<Arc<ClientConfig>> {
    for err in &errors {
        warn!("failed to load native certificate: {err}");
    }

    let mut root_store = RootCertStore::empty();
    let (added, ignored) = root_store.add_parsable_certificates(certs);
    if ignored > 0 {
        warn!("ignored {ignored} unparsable native certificate(s)");
    }

    if added == 0 {
        let mut err = Error::new(
            ErrorKind::Unexpected,
            "no usable native root certificates were loaded",
        );
        if let Some(source) = errors.into_iter().next() {
            err = err.set_source(source);
        }
        return Err(err);
    }

    let provider = CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(ring::default_provider()));

    let cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            Error::new(ErrorKind::Unexpected, "TLS protocols are unavailable").set_source(e)
        })?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Arc::new(cfg))
}

async fn create_root_dirs(
    ftp_stream: &mut AsyncRustlsFtpStream,
    root: &str,
) -> std::result::Result<(), FtpError> {
    let mut curr_path = if root.starts_with('/') {
        String::from("/")
    } else {
        String::new()
    };

    for segment in root.split('/').filter(|s| !s.is_empty()) {
        curr_path.push_str(segment);
        curr_path.push('/');
        match ftp_stream.mkdir(&curr_path).await {
            Ok(()) => (),
            Err(FtpError::UnexpectedResponse(Response {
                status: Status::FileUnavailable,
                ..
            })) => (),
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

pub struct FtpCore {
    info: ServiceInfo,
    capability: Capability,
    pool: Arc<bounded::Pool<Manager>>,
    /// The directory every pooled connection changes into after login.
    root: String,
}

impl FtpCore {
    pub fn new(info: ServiceInfo, capability: Capability, manager: Manager) -> Self {
        let root = manager.root.clone();
        let pool = bounded::Pool::new(bounded::PoolConfig::new(64), manager);
        Self {
            info,
            capability,
            pool,
            root,
        }
    }

    /// Check whether `path` is a directory by changing into it.
    ///
    /// Pooled connections rely on staying in `root`, since every other path
    /// is relative to it. After a successful `CWD` the connection changes
    /// back to `root` before it returns to the pool. A `550` reply means the
    /// directory does not exist and leaves the working directory unchanged.
    /// Any other error is returned. If an error other than `550` occurs,
    /// changing back fails, or the future is dropped in between, the
    /// connection is detached and closed instead of being reused.
    pub async fn ftp_dir_exists(&self, conn: bounded::Object<Manager>, path: &str) -> Result<bool> {
        struct ReturnGuard {
            conn: Option<bounded::Object<Manager>>,
            in_root: bool,
        }

        impl Drop for ReturnGuard {
            fn drop(&mut self) {
                if !self.in_root
                    && let Some(conn) = self.conn.take()
                {
                    drop(bounded::Object::detach(conn));
                }
            }
        }

        let mut guard = ReturnGuard {
            conn: Some(conn),
            in_root: false,
        };
        let conn = guard.conn.as_mut().expect("connection is set");
        match conn.cwd(path).await {
            Ok(()) => {
                conn.cwd(&self.root).await.map_err(format_ftp_error)?;
                guard.in_root = true;
                Ok(true)
            }
            Err(FtpError::UnexpectedResponse(resp)) if resp.status == Status::FileUnavailable => {
                guard.in_root = true;
                Ok(false)
            }
            Err(err) => Err(format_ftp_error(err)),
        }
    }

    pub fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    pub fn capability(&self) -> Capability {
        self.capability
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
    pub host: String,
    pub root: String,
    pub user: String,
    pub password: String,
    pub tls_config: Option<Arc<ClientConfig>>,
}

impl ManageObject for Manager {
    type Object = AsyncRustlsFtpStream;
    type Error = FtpError;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        let stream = ImplAsyncFtpStream::connect(&self.endpoint).await?;
        // switch to secure mode if ssl/tls is on.
        let mut ftp_stream = if let Some(cfg) = &self.tls_config {
            stream
                .into_secure(
                    AsyncRustlsConnector::from(TlsConnector::from(cfg.clone())),
                    &self.host,
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
                create_root_dirs(&mut ftp_stream, &self.root).await?;
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

mod err {
    use suppaftp::FtpError;
    use suppaftp::Status;

    use opendal_core::Error;
    use opendal_core::ErrorKind;

    pub(crate) fn format_ftp_error(err: FtpError) -> Error {
        let (kind, retryable) = match &err {
            FtpError::UnexpectedResponse(resp) => match resp.status {
                Status::NotAvailable => (ErrorKind::Unexpected, true),
                Status::NotLoggedIn => (ErrorKind::PermissionDenied, false),
                Status::FileUnavailable => (ErrorKind::NotFound, false),
                _ => (ErrorKind::Unexpected, false),
            },
            FtpError::BadResponse => (ErrorKind::Unexpected, true),
            _ => (ErrorKind::Unexpected, false),
        };

        let mut err = Error::new(kind, "ftp error").set_source(err);

        if retryable {
            err = err.set_temporary();
        }

        err
    }
}

pub(super) use err::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_tls_config_handles_empty_and_unparsable_certs_without_panicking() {
        let err = build_tls_config_from_parts(Vec::new(), Vec::new())
            .expect_err("empty cert store must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = build_tls_config_from_parts(
            vec![CertificateDer::from(vec![0xff, 0x00, 0xde, 0xad])],
            Vec::new(),
        )
        .expect_err("unparsable-only store must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let mut native = rustls_native_certs::load_native_certs();
        if !native.certs.is_empty() {
            native
                .certs
                .insert(0, CertificateDer::from(vec![0xff, 0x00, 0xde, 0xad]));
            assert!(
                build_tls_config(native).is_ok(),
                "unparsable cert alongside valid native certs must be ignored"
            );
        }
    }
}
