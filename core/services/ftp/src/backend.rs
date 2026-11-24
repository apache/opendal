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

use http::Uri;
use log::debug;
use suppaftp::FtpError;
use suppaftp::Status;
use suppaftp::list::File;
use suppaftp::types::Response;

use super::FTP_SCHEME;
use super::config::FtpConfig;
use super::core::FtpCore;
use super::core::Manager;
use super::core::build_tls_config;
use super::core::format_ftp_error;
use super::deleter::FtpDeleter;
use super::lister::parse_list_line;
use super::reader::*;
use opendal_core::raw::*;
use opendal_core::*;

/// FTP and FTPS services support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct FtpBuilder {
    pub(super) config: FtpConfig,
}

impl FtpBuilder {
    /// set endpoint for ftp backend.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// set root path for ftp backend.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// set user for ftp backend.
    pub fn user(mut self, user: &str) -> Self {
        self.config.user = if user.is_empty() {
            None
        } else {
            Some(user.to_string())
        };

        self
    }

    /// set password for ftp backend.
    pub fn password(mut self, password: &str) -> Self {
        self.config.password = if password.is_empty() {
            None
        } else {
            Some(password.to_string())
        };

        self
    }
}

impl Builder for FtpBuilder {
    type Config = FtpConfig;

    fn build(self) -> Result<impl Service> {
        debug!("ftp backend build started: {:?}", self);
        let endpoint = match &self.config.endpoint {
            None => return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")),
            Some(v) => v,
        };

        let endpoint_uri = match endpoint.parse::<Uri>() {
            Err(e) => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                    .with_context("endpoint", endpoint)
                    .set_source(e));
            }
            Ok(uri) => uri,
        };

        let host = endpoint_uri.host().unwrap_or("127.0.0.1");
        let port = endpoint_uri.port_u16().unwrap_or(21);

        let endpoint = format!("{host}:{port}");

        let enable_secure = match endpoint_uri.scheme_str() {
            Some("ftp") => false,
            // if the user forgot to add a scheme prefix
            // treat it as using secured scheme
            Some("ftps") | None => true,

            Some(s) => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "endpoint is unsupported or invalid",
                )
                .with_context("endpoint", s));
            }
        };

        let root = normalize_root(&self.config.root.unwrap_or_default());

        let user = match &self.config.user {
            None => "".to_string(),
            Some(v) => v.clone(),
        };

        let password = match &self.config.password {
            None => "".to_string(),
            Some(v) => v.clone(),
        };

        let info = ServiceInfo::new(FTP_SCHEME, &root, "");
        let capability = Capability {
            stat: true,

            read: true,

            write: true,
            write_can_empty: true,
            write_can_multi: true,
            write_can_append: true,

            delete: true,
            create_dir: true,

            list: true,

            shared: true,

            ..Default::default()
        };

        let tls_config = if enable_secure {
            Some(build_tls_config(rustls_native_certs::load_native_certs())?)
        } else {
            None
        };

        let manager = Manager {
            endpoint: endpoint.clone(),
            host: host.to_string(),
            root: root.clone(),
            user: user.clone(),
            password: password.clone(),
            tls_config,
        };

        let core = Arc::new(FtpCore::new(info, capability, manager.clone()));
        Ok(FtpBackend { core })
    }
}

#[derive(Clone)]
pub struct FtpBackend {
    pub(crate) core: Arc<FtpCore>,
}

impl Debug for FtpBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtpBackend").finish()
    }
}

impl Service for FtpBackend {
    type Reader = oio::StreamReader<FtpReader>;
    type Writer = FtpLazyWriter;
    type Lister = FtpLazyLister;
    type Deleter = oio::OneShotDeleter<FtpDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info()
    }

    fn capability(&self) -> Capability {
        self.core.capability()
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let mut ftp_stream = self.core.ftp_connect(Operation::CreateDir).await?;

        let paths: Vec<&str> = path.split_inclusive('/').collect();

        let mut curr_path = String::new();

        for path in paths {
            curr_path.push_str(path);
            match ftp_stream.mkdir(&curr_path).await {
                // Do nothing if status is FileUnavailable or OK(()) is return.
                Err(FtpError::UnexpectedResponse(Response {
                    status: Status::FileUnavailable,
                    ..
                }))
                | Ok(()) => (),
                Err(e) => {
                    return Err(format_ftp_error(e));
                }
            }
        }

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let file = self.ftp_stat(path).await?;

        let mode = if file.is_file() {
            EntryMode::FILE
        } else if file.is_directory() {
            EntryMode::DIR
        } else {
            EntryMode::Unknown
        };

        let mut meta = match mode {
            EntryMode::FILE => MetadataBuilder::file(file.size() as u64),
            EntryMode::DIR => MetadataBuilder::dir(),
            EntryMode::Unknown => MetadataBuilder::unknown(),
        };
        meta.last_modified(Timestamp::try_from(file.modified())?);

        Ok(RpStat::new(meta.build()))
    }
    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<FtpReader> = {
            Ok(oio::StreamReader::new(FtpReader::new(
                self.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, op: OpWrite) -> Result<Self::Writer> {
        Ok(FtpLazyWriter::new(self.core.clone(), path, op))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<FtpDeleter> =
            { Ok(oio::OneShotDeleter::new(FtpDeleter::new(self.core.clone()))) }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
        Ok(FtpLazyLister::new(self.core.clone(), path))
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

impl FtpBackend {
    pub async fn ftp_stat(&self, path: &str) -> Result<File> {
        let mut ftp_stream = self.core.ftp_connect(Operation::Stat).await?;

        let (parent, basename) = (get_parent(path), get_basename(path));

        let pathname = if parent == "/" { None } else { Some(parent) };

        let resp = ftp_stream.list(pathname).await.map_err(format_ftp_error)?;

        // Get stat of file.
        let mut files = resp
            .into_iter()
            .filter_map(|file| parse_list_line(file.as_str()).ok().flatten())
            .filter(|f| f.name() == basename.trim_end_matches('/'))
            .collect::<Vec<File>>();

        if files.is_empty() {
            Err(Error::new(
                ErrorKind::NotFound,
                "file is not found during list",
            ))
        } else {
            Ok(files.remove(0))
        }
    }
}

#[cfg(test)]
mod build_test {
    use super::FtpBuilder;
    use crate::FtpConfig;
    use opendal_core::*;

    #[test]
    fn test_build() {
        // ftps scheme, should suffix with default port 21
        let b = FtpBuilder::default()
            .endpoint("ftps://ftp_server.local")
            .build();
        assert!(b.is_ok());

        // ftp scheme
        let b = FtpBuilder::default()
            .endpoint("ftp://ftp_server.local:1234")
            .build();
        assert!(b.is_ok());

        // no scheme
        let b = FtpBuilder::default()
            .endpoint("ftp_server.local:8765")
            .build();
        assert!(b.is_ok());

        // invalid scheme
        let b = FtpBuilder::default()
            .endpoint("invalidscheme://ftp_server.local:8765")
            .build();
        assert!(b.is_err());
        let e = b.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn from_uri_sets_endpoint_and_root() {
        let uri = OperatorUri::new(
            "ftp://example.com/public/data",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = FtpConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("ftp://example.com"));
        assert_eq!(cfg.root.as_deref(), Some("public/data"));
    }

    #[test]
    fn from_uri_applies_credentials_from_query() {
        let uri = OperatorUri::new(
            "ftp://example.com/data",
            vec![
                ("user".to_string(), "alice".to_string()),
                ("password".to_string(), "secret".to_string()),
            ],
        )
        .unwrap();

        let cfg = FtpConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("ftp://example.com"));
        assert_eq!(cfg.user.as_deref(), Some("alice"));
        assert_eq!(cfg.password.as_deref(), Some("secret"));
    }
}

#[cfg(test)]
mod protocol_tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use opendal_core::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    use super::FtpBuilder;

    struct MockFtpState {
        reject_login: bool,
        dirs: HashSet<String>,
        listings: HashMap<String, Vec<String>>,
        mkd_calls: Vec<String>,
    }

    impl MockFtpState {
        fn new() -> Self {
            Self {
                reject_login: false,
                dirs: HashSet::from(["/".to_string()]),
                listings: HashMap::new(),
                mkd_calls: Vec::new(),
            }
        }
    }

    async fn spawn_mock_ftp(
        state: Arc<Mutex<MockFtpState>>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    if writer.write_all(b"220 mock ftp ready\r\n").await.is_err() {
                        return;
                    }

                    let mut data_listener: Option<TcpListener> = None;

                    while let Ok(Some(line)) = lines.next_line().await {
                        let (cmd, arg) = match line.split_once(' ') {
                            Some((c, a)) => (c.to_ascii_uppercase(), a.trim().to_string()),
                            None => (line.trim().to_ascii_uppercase(), String::new()),
                        };

                        match cmd.as_str() {
                            "USER" => {
                                let _ = writer.write_all(b"331 password required\r\n").await;
                            }
                            "PASS" => {
                                let reject = state.lock().unwrap().reject_login;
                                if reject {
                                    let _ = writer.write_all(b"530 Login incorrect.\r\n").await;
                                } else {
                                    let _ = writer.write_all(b"230 Login successful.\r\n").await;
                                }
                            }
                            "TYPE" | "NOOP" => {
                                let _ = writer.write_all(b"200 OK\r\n").await;
                            }
                            "CWD" => {
                                let exists = state.lock().unwrap().dirs.contains(&arg);
                                if exists {
                                    let _ = writer.write_all(b"250 CWD ok\r\n").await;
                                } else {
                                    let _ = writer.write_all(b"550 No such directory\r\n").await;
                                }
                            }
                            "MKD" => {
                                let existed = {
                                    let mut guard = state.lock().unwrap();
                                    guard.mkd_calls.push(arg.clone());
                                    !guard.dirs.insert(arg)
                                };
                                if existed {
                                    let _ = writer.write_all(b"550 Already exists\r\n").await;
                                } else {
                                    let _ = writer.write_all(b"257 Created\r\n").await;
                                }
                            }
                            "PASV" => {
                                let dl = TcpListener::bind("127.0.0.1:0").await.unwrap();
                                let port = dl.local_addr().unwrap().port();
                                data_listener = Some(dl);
                                let resp = format!(
                                    "227 Entering Passive Mode (127,0,0,1,{},{}).\r\n",
                                    port >> 8,
                                    port & 0xff
                                );
                                let _ = writer.write_all(resp.as_bytes()).await;
                            }
                            "LIST" => {
                                let key = if arg.is_empty() {
                                    "/".to_string()
                                } else {
                                    arg.clone()
                                };
                                let payload = state.lock().unwrap().listings.get(&key).cloned();
                                match payload {
                                    Some(entries) => {
                                        let dl = data_listener.take().unwrap();
                                        let _ = writer
                                            .write_all(b"150 Opening data connection\r\n")
                                            .await;
                                        if let Ok((mut data_sock, _)) = dl.accept().await {
                                            for entry in entries {
                                                let _ = data_sock.write_all(entry.as_bytes()).await;
                                                let _ = data_sock.write_all(b"\r\n").await;
                                            }
                                            let _ = data_sock.shutdown().await;
                                        }
                                        let _ =
                                            writer.write_all(b"226 Transfer complete\r\n").await;
                                    }
                                    None => {
                                        data_listener = None;
                                        let _ =
                                            writer.write_all(b"550 Directory not found\r\n").await;
                                    }
                                }
                            }
                            "RMD" | "DELE" | "SIZE" => {
                                let _ = writer.write_all(b"550 Operation failed\r\n").await;
                            }
                            "QUIT" => {
                                let _ = writer.write_all(b"221 Bye\r\n").await;
                                break;
                            }
                            _ => {
                                let _ = writer.write_all(b"502 Not implemented\r\n").await;
                            }
                        }
                    }
                });
            }
        });
        (format!("ftp://127.0.0.1:{}", addr.port()), handle)
    }

    #[tokio::test]
    async fn test_list_handles_550_missing_dir_and_recursive_dots_and_total() {
        let state = Arc::new(Mutex::new(MockFtpState::new()));
        {
            let mut guard = state.lock().unwrap();
            guard.listings.insert(
                "dir/".to_string(),
                vec![
                    "total 8".to_string(),
                    "drwxr-xr-x 2 user group 4096 Nov  5  2018 .".to_string(),
                    "drwxr-xr-x 4 user group 4096 Nov  5  2018 ..".to_string(),
                    "drwxr-xr-x 2 user group 4096 Nov  5  2018 sub".to_string(),
                    "-rw-r--r-- 1 user group   10 Nov  5  2018 a.txt".to_string(),
                ],
            );
            guard.listings.insert(
                "dir/sub/".to_string(),
                vec![
                    "total 4".to_string(),
                    "drwxr-xr-x 2 user group 4096 Nov  5  2018 .".to_string(),
                    "drwxr-xr-x 2 user group 4096 Nov  5  2018 ..".to_string(),
                    "-rw-r--r-- 1 user group    5 Nov  5  2018 b.txt".to_string(),
                ],
            );
        }

        let (endpoint, server) = spawn_mock_ftp(state).await;
        let op = Operator::new(FtpBuilder::default().endpoint(&endpoint).root("/"))
            .unwrap()
            .layer(layers::SimulateLayer::default());

        let missing = op.list("missing/").await.unwrap();
        assert!(missing.is_empty(), "550 on missing dir must list as empty");

        let entries = op.list_with("dir/").recursive(true).await.unwrap();
        let mut paths: Vec<String> = entries.into_iter().map(|e| e.path().to_string()).collect();
        paths.sort();
        assert_eq!(
            paths,
            vec!["dir/", "dir/a.txt", "dir/sub/", "dir/sub/b.txt"]
        );

        server.abort();
    }

    #[tokio::test]
    async fn test_delete_550_distinguishes_existing_from_missing() {
        let state = Arc::new(Mutex::new(MockFtpState::new()));
        {
            let mut guard = state.lock().unwrap();
            guard.listings.insert(
                "/".to_string(),
                vec![
                    "total 8".to_string(),
                    "drwxr-xr-x 2 user group 4096 Nov  5  2018 nonempty".to_string(),
                    "-rw-r--r-- 1 user group   10 Nov  5  2018 readonly.txt".to_string(),
                ],
            );
        }

        let (endpoint, server) = spawn_mock_ftp(state).await;
        let op = Operator::new(FtpBuilder::default().endpoint(&endpoint).root("/")).unwrap();

        let err = op
            .delete("nonempty/")
            .await
            .expect_err("non-empty dir must not silently succeed on 550");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = op
            .delete("readonly.txt")
            .await
            .expect_err("existing file must not silently succeed on 550");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let err = op
            .delete("readonly.txt/")
            .await
            .expect_err("deleting a file via dir path must fail");
        assert_eq!(err.kind(), ErrorKind::NotADirectory);

        let err = op
            .delete("nonempty")
            .await
            .expect_err("deleting a dir via file path must fail");
        assert_eq!(err.kind(), ErrorKind::IsADirectory);

        op.delete("missing.txt")
            .await
            .expect("missing file delete must be idempotent");
        op.delete("missing_dir/")
            .await
            .expect("missing dir delete must be idempotent");
        op.delete("missing_parent/file.txt")
            .await
            .expect("delete under missing parent must be idempotent");

        server.abort();
    }

    #[tokio::test]
    async fn test_manager_creates_multilevel_root_and_maps_530_permission_denied() {
        let state = Arc::new(Mutex::new(MockFtpState::new()));
        let (endpoint, server) = spawn_mock_ftp(state.clone()).await;

        let op = Operator::new(FtpBuilder::default().endpoint(&endpoint).root("/a/b/c")).unwrap();
        let entries = op.list("/").await.unwrap();
        assert!(entries.is_empty());

        let mkd_calls = state.lock().unwrap().mkd_calls.clone();
        assert_eq!(mkd_calls, vec!["/a/", "/a/b/", "/a/b/c/"]);

        state.lock().unwrap().reject_login = true;
        let bad_op = Operator::new(
            FtpBuilder::default()
                .endpoint(&endpoint)
                .user("alice")
                .password("wrong"),
        )
        .unwrap();
        let err = bad_op
            .list("/")
            .await
            .expect_err("530 login failure must fail");
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(!err.is_temporary());

        server.abort();
    }
}
