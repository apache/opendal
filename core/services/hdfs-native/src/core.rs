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

use std::fmt::Debug;
use std::sync::Arc;

use hdfs_native::HdfsError;
use hdfs_native::WriteOptions;

use opendal_core::raw::*;
use opendal_core::*;

/// HdfsNativeCore contains code that directly interacts with HDFS Native client.
#[derive(Clone)]
pub struct HdfsNativeCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub root: String,
    pub client: Arc<hdfs_native::Client>,
}

impl Debug for HdfsNativeCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsNativeCore")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl HdfsNativeCore {
    pub async fn hdfs_create_dir(&self, path: &str) -> Result<()> {
        let p = build_rooted_absolute_path(&self.root, path);

        self.client
            .mkdirs(&p, 0o777, true)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(())
    }

    pub async fn hdfs_stat(&self, path: &str) -> Result<Metadata> {
        let p = build_rooted_absolute_path(&self.root, path);

        let status: hdfs_native::client::FileStatus = self
            .client
            .get_file_info(&p)
            .await
            .map_err(parse_hdfs_error)?;

        let mode = if status.isdir {
            EntryMode::DIR
        } else {
            EntryMode::FILE
        };

        let mut metadata = if mode == EntryMode::FILE {
            MetadataBuilder::file(status.length as u64)
        } else {
            MetadataBuilder::dir()
        };
        metadata.last_modified(Timestamp::from_millisecond(
            status.modification_time as i64,
        )?);

        Ok(metadata.build())
    }

    pub async fn hdfs_open(&self, path: &str) -> Result<hdfs_native::file::FileReader> {
        let p = build_rooted_absolute_path(&self.root, path);

        self.client.read(&p).await.map_err(parse_hdfs_error)
    }

    pub async fn hdfs_write(
        &self,
        path: &str,
        args: &OpWrite,
    ) -> Result<(hdfs_native::file::FileWriter, u64)> {
        let target_path = build_rooted_absolute_path(&self.root, path);
        let mut initial_size = 0;

        let target_exists = match self.client.get_file_info(&target_path).await {
            Ok(status) => {
                initial_size = status.length as u64;
                true
            }
            Err(err) => match &err {
                HdfsError::FileNotFound(_) => false,
                _ => return Err(parse_hdfs_error(err)),
            },
        };

        let f = if target_exists {
            if args.append() {
                self.client
                    .append(&target_path)
                    .await
                    .map_err(parse_hdfs_error)?
            } else {
                initial_size = 0;
                self.client
                    .create(&target_path, WriteOptions::default().overwrite(true))
                    .await
                    .map_err(parse_hdfs_error)?
            }
        } else {
            initial_size = 0;
            self.client
                .create(&target_path, WriteOptions::default())
                .await
                .map_err(parse_hdfs_error)?
        };

        Ok((f, initial_size))
    }

    pub async fn hdfs_delete(&self, path: &str, recursive: bool) -> Result<()> {
        let p = build_rooted_absolute_path(&self.root, path);

        self.client
            .delete(&p, recursive)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(())
    }

    pub async fn hdfs_list(&self, path: &str) -> Result<Option<(String, Option<String>)>> {
        let p: String = build_rooted_absolute_path(&self.root, path);

        let isdir = match self.client.get_file_info(&p).await {
            Ok(status) => status.isdir,
            Err(err) => {
                return match &err {
                    HdfsError::FileNotFound(_) => Ok(None),
                    _ => Err(parse_hdfs_error(err)),
                };
            }
        };

        let current_path = if isdir {
            if !path.ends_with("/") {
                Some(path.to_string() + "/")
            } else {
                Some(path.to_string())
            }
        } else {
            None
        };

        Ok(Some((p, current_path)))
    }

    pub async fn hdfs_rename(&self, from: &str, to: &str, args: &OpRename) -> Result<()> {
        let from_path = build_rooted_absolute_path(&self.root, from);
        let to_path = build_rooted_absolute_path(&self.root, to);

        let source = self
            .client
            .get_file_info(&from_path)
            .await
            .map_err(parse_hdfs_error)?;
        if source.isdir {
            return Err(Error::new(ErrorKind::IsADirectory, "path should be a file")
                .with_context("input", &from_path));
        }

        let target_exists = match self.client.get_file_info(&to_path).await {
            Ok(status) => {
                if status.isdir {
                    return Err(Error::new(ErrorKind::IsADirectory, "path should be a file")
                        .with_context("input", &to_path));
                }
                true
            }
            Err(err) => match &err {
                HdfsError::FileNotFound(_) => false,
                _ => return Err(parse_hdfs_error(err)),
            },
        };

        if target_exists {
            if args.if_not_exists() {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "target path already exists while if_not_exists is set",
                )
                .with_context("input", &to_path));
            }

            return self
                .client
                .rename(&from_path, &to_path, true)
                .await
                .map_err(parse_hdfs_error);
        }

        if let Some((parent, _)) = to_path
            .rsplit_once('/')
            .filter(|(parent, _)| !parent.is_empty())
        {
            let parent_exists = match self.client.get_file_info(parent).await {
                Ok(_) => true,
                Err(err) => match &err {
                    HdfsError::FileNotFound(_) => false,
                    _ => return Err(parse_hdfs_error(err)),
                },
            };
            if !parent_exists {
                self.client
                    .mkdirs(parent, 0o777, true)
                    .await
                    .map_err(parse_hdfs_error)?;
            }
        }

        self.client
            .rename(&from_path, &to_path, !args.if_not_exists())
            .await
            .map_err(|err| {
                if args.if_not_exists() && matches!(&err, HdfsError::AlreadyExists(_)) {
                    Error::new(
                        ErrorKind::ConditionNotMatch,
                        "target path already exists while if_not_exists is set",
                    )
                    .with_context("input", &to_path)
                    .set_source(err)
                } else {
                    parse_hdfs_error(err)
                }
            })?;

        Ok(())
    }
}

fn classify_rpc_error(class: &str) -> (ErrorKind, bool) {
    if class.contains("FileNotFoundException") {
        (ErrorKind::NotFound, false)
    } else if class.contains("AccessControlException")
        || class.contains("AuthorizationException")
        || class.contains("SaslException")
        || class.contains("GSSException")
        || class.contains("InvalidToken")
    {
        (ErrorKind::PermissionDenied, false)
    } else if class.contains("ParentNotDirectoryException") {
        (ErrorKind::NotADirectory, false)
    } else if class.contains("RetriableException")
        || class.contains("StandbyException")
        || class.contains("ObserverRetryOnActiveException")
        || class.contains("SafeModeException")
    {
        (ErrorKind::Unexpected, true)
    } else {
        (ErrorKind::Unexpected, false)
    }
}

/// Parse hdfs-native error into opendal::Error.
pub fn parse_hdfs_error(hdfs_error: HdfsError) -> Error {
    let (kind, retryable, msg) = match &hdfs_error {
        HdfsError::IOError(err) => {
            let (kind, retryable) = match err.kind() {
                std::io::ErrorKind::NotFound => (ErrorKind::NotFound, false),
                std::io::ErrorKind::PermissionDenied => (ErrorKind::PermissionDenied, false),
                std::io::ErrorKind::AlreadyExists => (ErrorKind::AlreadyExists, false),
                std::io::ErrorKind::IsADirectory => (ErrorKind::IsADirectory, false),
                std::io::ErrorKind::NotADirectory => (ErrorKind::NotADirectory, false),
                std::io::ErrorKind::Unsupported => (ErrorKind::Unsupported, false),
                std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::HostUnreachable
                | std::io::ErrorKind::NetworkUnreachable
                | std::io::ErrorKind::AddrNotAvailable => (ErrorKind::Unexpected, true),
                _ => (ErrorKind::Unexpected, false),
            };
            (kind, retryable, err.to_string())
        }
        HdfsError::DataTransferError(msg) => (ErrorKind::Unexpected, false, msg.clone()),
        HdfsError::ChecksumError => (ErrorKind::Unexpected, false, hdfs_error.to_string()),
        HdfsError::UrlParseError(err) => (ErrorKind::Unexpected, false, err.to_string()),
        HdfsError::AlreadyExists(msg) => (ErrorKind::AlreadyExists, false, msg.clone()),
        HdfsError::OperationFailed(msg) => (ErrorKind::Unexpected, false, msg.clone()),
        HdfsError::RPCError(class, msg) | HdfsError::FatalRPCError(class, msg) => {
            let (kind, retryable) = classify_rpc_error(class);
            let message = if msg.is_empty() {
                class.clone()
            } else {
                format!("{class}: {msg}")
            };
            (kind, retryable, message)
        }
        HdfsError::SASLError(_) | HdfsError::GSSAPIError(..) | HdfsError::NoSASLMechanism => {
            (ErrorKind::PermissionDenied, false, hdfs_error.to_string())
        }
        HdfsError::UnsupportedFeature(_) | HdfsError::UnsupportedErasureCodingPolicy(_) => {
            (ErrorKind::Unsupported, false, hdfs_error.to_string())
        }
        HdfsError::FileNotFound(msg) => (ErrorKind::NotFound, false, msg.clone()),
        HdfsError::BlocksNotFound(msg) => (ErrorKind::NotFound, false, msg.clone()),
        HdfsError::IsADirectoryError(msg) => (ErrorKind::IsADirectory, false, msg.clone()),
        _ => (ErrorKind::Unexpected, false, hdfs_error.to_string()),
    };

    let mut err = Error::new(kind, msg).set_source(hdfs_error);

    if retryable {
        err = err.set_temporary();
    }

    err
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn parse_rpc_errors_classifies_java_exceptions_and_preserves_context() {
        let cases = [
            (
                "java.io.FileNotFoundException",
                "file missing",
                ErrorKind::NotFound,
                false,
            ),
            (
                "org.apache.hadoop.security.AccessControlException",
                "permission denied for /secret",
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                "org.apache.hadoop.security.authorize.AuthorizationException",
                "user is not authorized",
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                "javax.security.sasl.SaslException",
                "SASL negotiation failed",
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                "org.ietf.jgss.GSSException",
                "ticket expired",
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                "org.apache.hadoop.security.token.SecretManager$InvalidToken",
                "invalid delegation token",
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                "org.apache.hadoop.fs.ParentNotDirectoryException",
                "/a/b is not a directory",
                ErrorKind::NotADirectory,
                false,
            ),
            (
                "org.apache.hadoop.ipc.RetriableException",
                "please retry",
                ErrorKind::Unexpected,
                true,
            ),
            (
                "org.apache.hadoop.ipc.StandbyException",
                "NameNode is in standby",
                ErrorKind::Unexpected,
                true,
            ),
            (
                "org.apache.hadoop.ipc.ObserverRetryOnActiveException",
                "retry on active",
                ErrorKind::Unexpected,
                true,
            ),
            (
                "org.apache.hadoop.hdfs.server.namenode.SafeModeException",
                "NameNode is in safe mode",
                ErrorKind::Unexpected,
                true,
            ),
            (
                "org.apache.hadoop.hdfs.protocol.NSQuotaExceededException",
                "quota exceeded",
                ErrorKind::Unexpected,
                false,
            ),
        ];

        for (class, msg, expected_kind, expected_temp) in cases {
            for hdfs_err in [
                HdfsError::RPCError(class.to_string(), msg.to_string()),
                HdfsError::FatalRPCError(class.to_string(), msg.to_string()),
            ] {
                let err = parse_hdfs_error(hdfs_err);
                assert_eq!(err.kind(), expected_kind, "class={class}");
                assert_eq!(err.is_temporary(), expected_temp, "class={class}");
                let rendered = err.to_string();
                assert!(rendered.contains(class), "missing class in {rendered}");
                assert!(rendered.contains(msg), "missing msg in {rendered}");
            }
        }
    }

    #[test]
    fn parse_auth_and_unsupported_errors() {
        let sasl = parse_hdfs_error(HdfsError::SASLError("bad handshake".to_string()));
        assert_eq!(sasl.kind(), ErrorKind::PermissionDenied);
        assert!(!sasl.is_temporary());
        assert!(sasl.to_string().contains("bad handshake"));

        let no_mech = parse_hdfs_error(HdfsError::NoSASLMechanism);
        assert_eq!(no_mech.kind(), ErrorKind::PermissionDenied);
        assert!(!no_mech.is_temporary());
        assert!(no_mech.to_string().contains("No valid SASL mechanism"));

        let unsupported =
            parse_hdfs_error(HdfsError::UnsupportedFeature("hedged reads".to_string()));
        assert_eq!(unsupported.kind(), ErrorKind::Unsupported);
        assert!(!unsupported.is_temporary());
        assert!(unsupported.to_string().contains("hedged reads"));

        let ec = parse_hdfs_error(HdfsError::UnsupportedErasureCodingPolicy(
            "RS-10-4".to_string(),
        ));
        assert_eq!(ec.kind(), ErrorKind::Unsupported);
        assert!(!ec.is_temporary());
        assert!(ec.to_string().contains("RS-10-4"));
    }

    #[test]
    fn parse_io_errors_maps_permanent_and_temporary_kinds() {
        let cases = [
            (io::ErrorKind::NotFound, ErrorKind::NotFound, false),
            (
                io::ErrorKind::PermissionDenied,
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                io::ErrorKind::AlreadyExists,
                ErrorKind::AlreadyExists,
                false,
            ),
            (io::ErrorKind::IsADirectory, ErrorKind::IsADirectory, false),
            (
                io::ErrorKind::NotADirectory,
                ErrorKind::NotADirectory,
                false,
            ),
            (io::ErrorKind::Unsupported, ErrorKind::Unsupported, false),
            (io::ErrorKind::InvalidInput, ErrorKind::Unexpected, false),
            (io::ErrorKind::TimedOut, ErrorKind::Unexpected, true),
            (io::ErrorKind::Interrupted, ErrorKind::Unexpected, true),
            (
                io::ErrorKind::ConnectionRefused,
                ErrorKind::Unexpected,
                true,
            ),
            (io::ErrorKind::ConnectionReset, ErrorKind::Unexpected, true),
            (
                io::ErrorKind::ConnectionAborted,
                ErrorKind::Unexpected,
                true,
            ),
            (io::ErrorKind::BrokenPipe, ErrorKind::Unexpected, true),
            (io::ErrorKind::WouldBlock, ErrorKind::Unexpected, true),
            (io::ErrorKind::UnexpectedEof, ErrorKind::Unexpected, true),
        ];

        for (io_kind, expected_kind, expected_temp) in cases {
            let err = parse_hdfs_error(HdfsError::IOError(io::Error::new(
                io_kind,
                format!("detail for {io_kind:?}"),
            )));
            assert_eq!(err.kind(), expected_kind, "io_kind={io_kind:?}");
            assert_eq!(err.is_temporary(), expected_temp, "io_kind={io_kind:?}");
            assert!(err.to_string().contains(&format!("detail for {io_kind:?}")));
        }
    }

    #[test]
    fn parse_fallback_errors_preserve_original_message() {
        let cases = [
            (HdfsError::InvalidPath("/bad/path".to_string()), "/bad/path"),
            (
                HdfsError::InvalidArgument("missing fs.defaultFS".to_string()),
                "missing fs.defaultFS",
            ),
            (
                HdfsError::InternalError("unexpected block state".to_string()),
                "unexpected block state",
            ),
            (HdfsError::TrashNotEnabled, "Trash is not enabled"),
        ];

        for (hdfs_err, needle) in cases {
            let err = parse_hdfs_error(hdfs_err);
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(!err.is_temporary());
            assert!(
                err.to_string().contains(needle),
                "expected {needle:?} in {err}"
            );
        }
    }
}
