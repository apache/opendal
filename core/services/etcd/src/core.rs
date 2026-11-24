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

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::bounded;
use etcd_client::Client;
use etcd_client::ConnectOptions;
use etcd_client::Error as EtcdError;
use etcd_client::KvClient;
use std::fmt::Debug;
use std::sync::Arc;
use tonic::Code;

use opendal_core::raw::*;
use opendal_core::{Buffer, Error, ErrorKind, Result};

pub mod constants {
    pub const DEFAULT_ETCD_ENDPOINTS: &str = "http://127.0.0.1:2379";
}

#[derive(Clone)]
pub struct Manager {
    endpoints: Vec<String>,
    options: ConnectOptions,
}

impl ManageObject for Manager {
    type Object = Client;
    type Error = Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        Client::connect(self.endpoints.clone(), Some(self.options.clone()))
            .await
            .map_err(format_etcd_error)
    }

    async fn is_recyclable(
        &self,
        o: &mut Self::Object,
        _: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        match o.status().await {
            Ok(_) => Ok(()),
            Err(err) => Err(format_etcd_error(err)),
        }
    }
}

#[derive(Clone)]
pub struct EtcdCore {
    endpoints: Vec<String>,
    max_decoding_message_size: Option<usize>,
    client: Arc<bounded::Pool<Manager>>,
}

impl Debug for EtcdCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdCore")
            .field("endpoints", &self.endpoints.join(","))
            .field("max_decoding_message_size", &self.max_decoding_message_size)
            .finish_non_exhaustive()
    }
}

impl EtcdCore {
    pub fn new(
        endpoints: Vec<String>,
        options: ConnectOptions,
        max_decoding_message_size: Option<usize>,
    ) -> Self {
        let client = bounded::Pool::new(
            bounded::PoolConfig::new(64),
            Manager {
                endpoints: endpoints.clone(),
                options,
            },
        );

        Self {
            endpoints,
            max_decoding_message_size,
            client,
        }
    }

    #[cfg(test)]
    pub(crate) fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    pub async fn conn(&self) -> Result<bounded::Object<Manager>> {
        let fut = self.client.get();

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

    pub async fn has_prefix(&self, prefix: &str) -> Result<bool> {
        let client = self.conn().await?;
        let get_options = Some(
            etcd_client::GetOptions::new()
                .with_prefix()
                .with_keys_only()
                .with_limit(1),
        );
        let resp = self
            .kv_client(&client)
            .get(prefix, get_options)
            .await
            .map_err(format_etcd_error)?;
        Ok(!resp.kvs().is_empty())
    }

    /// Builds a KV client from `conn` with the configured response decoding limit applied.
    ///
    /// The limit lives on [`KvClient`] rather than on [`ConnectOptions`], so every KV
    /// request has to apply it to its own client.
    pub fn kv_client(&self, conn: &Client) -> KvClient {
        let kv = conn.kv_client();
        match self.max_decoding_message_size {
            Some(size) => kv.max_decoding_message_size(size),
            None => kv,
        }
    }
}

impl EtcdCore {
    pub async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let client = self.conn().await?;
        let resp = self
            .kv_client(&client)
            .get(key, None)
            .await
            .map_err(format_etcd_error)?;
        if let Some(kv) = resp.kvs().first() {
            Ok(Some(Buffer::from(kv.value().to_vec())))
        } else {
            Ok(None)
        }
    }

    pub async fn set(&self, key: &str, value: Buffer) -> Result<()> {
        let mut client = self.conn().await?;
        let _ = client
            .put(key, value.to_vec(), None)
            .await
            .map_err(format_etcd_error)?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut client = self.conn().await?;
        let _ = client.delete(key, None).await.map_err(format_etcd_error)?;
        Ok(())
    }
}

pub fn format_etcd_error(e: EtcdError) -> Error {
    let (kind, temporary) = match &e {
        EtcdError::GRpcStatus(status) => match status.code() {
            Code::PermissionDenied | Code::Unauthenticated => (ErrorKind::PermissionDenied, false),
            Code::InvalidArgument
                if status.message().contains("authentication failed")
                    || status.message().contains("user name is empty") =>
            {
                (ErrorKind::PermissionDenied, false)
            }
            Code::NotFound => (ErrorKind::NotFound, false),
            Code::AlreadyExists => (ErrorKind::AlreadyExists, false),
            Code::ResourceExhausted => (ErrorKind::RateLimited, true),
            Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Aborted
            | Code::Cancelled
            | Code::Unknown => (ErrorKind::Unexpected, true),
            _ => (ErrorKind::Unexpected, false),
        },
        EtcdError::InvalidArgs(_)
        | EtcdError::InvalidUri(_)
        | EtcdError::InvalidMetadataValue(_) => (ErrorKind::ConfigInvalid, false),
        EtcdError::TransportError(_) | EtcdError::IoError(_) => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let err = Error::new(kind, e.to_string()).set_source(e);
    if temporary { err.set_temporary() } else { err }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Status;

    #[test]
    fn debug_omits_connect_options_password() {
        let options = ConnectOptions::new().with_user("alice", "s3cr3t-pass");
        let core = EtcdCore::new(vec!["http://127.0.0.1:2379".to_string()], options, None);
        let debug_str = format!("{core:?}");
        assert!(debug_str.contains("http://127.0.0.1:2379"));
        assert!(!debug_str.contains("s3cr3t-pass"));
    }

    #[test]
    fn format_etcd_error_classifies_grpc_and_client_errors() {
        let cases = [
            (
                EtcdError::GRpcStatus(Status::unauthenticated("bad token")),
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::permission_denied("forbidden")),
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::invalid_argument(
                    "etcdserver: authentication failed, invalid user ID or password",
                )),
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::invalid_argument("etcdserver: user name is empty")),
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::not_found("missing")),
                ErrorKind::NotFound,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::already_exists("exists")),
                ErrorKind::AlreadyExists,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::resource_exhausted("quota exceeded")),
                ErrorKind::RateLimited,
                true,
            ),
            (
                EtcdError::GRpcStatus(Status::unavailable("leader election")),
                ErrorKind::Unexpected,
                true,
            ),
            (
                EtcdError::GRpcStatus(Status::deadline_exceeded("timeout")),
                ErrorKind::Unexpected,
                true,
            ),
            (
                EtcdError::GRpcStatus(Status::aborted("txn aborted")),
                ErrorKind::Unexpected,
                true,
            ),
            (
                EtcdError::GRpcStatus(Status::cancelled("cancelled")),
                ErrorKind::Unexpected,
                true,
            ),
            (
                EtcdError::GRpcStatus(Status::unknown("unknown")),
                ErrorKind::Unexpected,
                true,
            ),
            (
                EtcdError::GRpcStatus(Status::invalid_argument("key is not provided")),
                ErrorKind::Unexpected,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::failed_precondition("failed precondition")),
                ErrorKind::Unexpected,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::out_of_range("compacted")),
                ErrorKind::Unexpected,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::unimplemented("unimplemented")),
                ErrorKind::Unexpected,
                false,
            ),
            (
                EtcdError::GRpcStatus(Status::data_loss("corrupt")),
                ErrorKind::Unexpected,
                false,
            ),
            (
                EtcdError::InvalidArgs("empty endpoints".to_string()),
                ErrorKind::ConfigInvalid,
                false,
            ),
            (
                EtcdError::IoError(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "reset",
                )),
                ErrorKind::Unexpected,
                true,
            ),
            (
                EtcdError::Internal("internal error".to_string()),
                ErrorKind::Unexpected,
                false,
            ),
        ];

        for (input, expected_kind, expected_temporary) in cases {
            let err = format_etcd_error(input);
            assert_eq!(err.kind(), expected_kind, "error: {err}");
            assert_eq!(err.is_temporary(), expected_temporary, "error: {err}");
        }
    }
}
