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

use asyncband::once::OnceCell;

use super::TIKV_SCHEME;
use super::config::TikvConfig;
use super::core::*;
use super::deleter::TikvDeleter;
use super::reader::*;
use super::writer::TikvWriter;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

/// TiKV backend builder
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct TikvBuilder {
    pub(super) config: TikvConfig,
}

impl TikvBuilder {
    /// Set the network address of the TiKV service.
    pub fn endpoints(mut self, endpoints: Vec<String>) -> Self {
        let endpoints: Vec<String> = endpoints
            .into_iter()
            .map(|endpoint| endpoint.trim().to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect();
        if !endpoints.is_empty() {
            self.config.endpoints = Some(endpoints);
        }
        self
    }

    /// Set the insecure connection to TiKV.
    pub fn insecure(mut self) -> Self {
        self.config.insecure = true;
        self
    }

    /// Set the certificate authority file path.
    pub fn ca_path(mut self, ca_path: &str) -> Self {
        if !ca_path.is_empty() {
            self.config.ca_path = Some(ca_path.to_string())
        }
        self
    }

    /// Set the certificate file path.
    pub fn cert_path(mut self, cert_path: &str) -> Self {
        if !cert_path.is_empty() {
            self.config.cert_path = Some(cert_path.to_string())
        }
        self
    }

    /// Set the key file path.
    pub fn key_path(mut self, key_path: &str) -> Self {
        if !key_path.is_empty() {
            self.config.key_path = Some(key_path.to_string())
        }
        self
    }
}

impl Builder for TikvBuilder {
    type Config = TikvConfig;

    fn build(self) -> Result<impl Service> {
        let endpoints: Vec<String> = self
            .config
            .endpoints
            .unwrap_or_default()
            .into_iter()
            .map(|endpoint| endpoint.trim().to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect();

        if endpoints.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "endpoints is required but not set",
            )
            .with_context("service", TIKV_SCHEME));
        }

        let ca_path = self.config.ca_path.filter(|v| !v.is_empty());
        let cert_path = self.config.cert_path.filter(|v| !v.is_empty());
        let key_path = self.config.key_path.filter(|v| !v.is_empty());

        if self.config.insecure {
            if ca_path.is_some() || key_path.is_some() || cert_path.is_some() {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "invalid tls configuration")
                        .with_context("service", TIKV_SCHEME)
                        .with_context("endpoints", format!("{endpoints:?}")),
                );
            }
        } else if ca_path.is_none() || cert_path.is_none() || key_path.is_none() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "ca_path, cert_path, and key_path are required when insecure is false",
            )
            .with_context("service", TIKV_SCHEME)
            .with_context("endpoints", format!("{endpoints:?}")));
        }

        Ok(TikvBackend::new(TikvCore {
            client: OnceCell::new(),
            endpoints,
            insecure: self.config.insecure,
            ca_path,
            cert_path,
            key_path,
        }))
    }
}

/// Backend for TiKV service
#[derive(Clone, Debug)]
pub struct TikvBackend {
    pub(crate) core: Arc<TikvCore>,
    pub(crate) root: String,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl TikvBackend {
    fn new(core: TikvCore) -> Self {
        let info = ServiceInfo::new(TIKV_SCHEME, "/", "TiKV");
        let capability = Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
            shared: true,
            ..Default::default()
        };

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info,
            capability,
        }
    }
}

impl Service for TikvBackend {
    type Reader = oio::StreamReader<TikvReader>;
    type Writer = TikvWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<TikvDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_absolute_path(&self.root, path);

        if p == build_absolute_path(&self.root, "/") {
            Ok(RpStat::new(MetadataBuilder::dir().build()))
        } else {
            let bs = self.core.get(&p).await?;
            match bs {
                Some(bs) => {
                    let metadata = if p.ends_with('/') {
                        MetadataBuilder::dir()
                    } else {
                        MetadataBuilder::file(bs.len() as u64)
                    };
                    Ok(RpStat::new(metadata.build()))
                }
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in tikv")),
            }
        }
    }
    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<TikvReader> = {
            Ok(oio::StreamReader::new(TikvReader::new(
                self.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        let output: TikvWriter = {
            let p = build_absolute_path(&self.root, path);
            Ok(TikvWriter::new(self.core.clone(), p))
        }?;

        Ok(output)
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<TikvDeleter> = {
            Ok(oio::OneShotDeleter::new(TikvDeleter::new(
                self.core.clone(),
                self.root.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, _path: &str, _args: OpList) -> Result<Self::Lister> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
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

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::time::Duration;

    use super::*;

    #[test]
    fn build_rejects_missing_empty_or_blank_endpoints() {
        let cases = [
            TikvBuilder::default().insecure(),
            TikvBuilder::default().endpoints(vec![]).insecure(),
            TikvBuilder::default()
                .endpoints(vec!["".to_string(), "   ".to_string()])
                .insecure(),
            TikvConfig {
                endpoints: Some(vec![]),
                insecure: true,
                ..Default::default()
            }
            .into_builder(),
            TikvConfig {
                endpoints: Some(vec!["  ".to_string(), "\t".to_string()]),
                insecure: true,
                ..Default::default()
            }
            .into_builder(),
        ];

        for builder in cases {
            let err = builder.build().expect_err("build should fail");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
            assert!(!err.is_temporary());
        }

        let service = TikvBuilder::default()
            .endpoints(vec!["  127.0.0.1:2379  ".to_string(), "".to_string()])
            .insecure()
            .build();
        assert!(service.is_ok());
    }

    #[test]
    fn build_validates_tls_configuration() {
        let invalid_cases = [
            // Default (!insecure) requires all three TLS paths.
            TikvBuilder::default().endpoints(vec!["127.0.0.1:2379".to_string()]),
            TikvBuilder::default()
                .endpoints(vec!["127.0.0.1:2379".to_string()])
                .ca_path("/ca.pem"),
            TikvBuilder::default()
                .endpoints(vec!["127.0.0.1:2379".to_string()])
                .ca_path("/ca.pem")
                .cert_path("/cert.pem"),
            TikvBuilder::default()
                .endpoints(vec!["127.0.0.1:2379".to_string()])
                .ca_path("/ca.pem")
                .key_path("/key.pem"),
            TikvBuilder::default()
                .endpoints(vec!["127.0.0.1:2379".to_string()])
                .cert_path("/cert.pem")
                .key_path("/key.pem"),
            // Empty string TLS paths in config are treated as unset.
            TikvConfig {
                endpoints: Some(vec!["127.0.0.1:2379".to_string()]),
                insecure: false,
                ca_path: Some("".to_string()),
                cert_path: Some("/cert.pem".to_string()),
                key_path: Some("/key.pem".to_string()),
            }
            .into_builder(),
            TikvConfig {
                endpoints: Some(vec!["127.0.0.1:2379".to_string()]),
                insecure: false,
                ca_path: Some("/ca.pem".to_string()),
                cert_path: Some("".to_string()),
                key_path: Some("/key.pem".to_string()),
            }
            .into_builder(),
            TikvConfig {
                endpoints: Some(vec!["127.0.0.1:2379".to_string()]),
                insecure: false,
                ca_path: Some("/ca.pem".to_string()),
                cert_path: Some("/cert.pem".to_string()),
                key_path: Some("".to_string()),
            }
            .into_builder(),
            // Insecure cannot be combined with non-empty TLS paths.
            TikvBuilder::default()
                .endpoints(vec!["127.0.0.1:2379".to_string()])
                .insecure()
                .ca_path("/ca.pem"),
            TikvBuilder::default()
                .endpoints(vec!["127.0.0.1:2379".to_string()])
                .insecure()
                .cert_path("/cert.pem"),
            TikvBuilder::default()
                .endpoints(vec!["127.0.0.1:2379".to_string()])
                .insecure()
                .key_path("/key.pem"),
        ];

        for builder in invalid_cases {
            let err = builder.build().expect_err("build should fail");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
            assert!(!err.is_temporary());
        }

        // Valid insecure configs (including empty string TLS paths from config maps).
        assert!(
            TikvBuilder::default()
                .endpoints(vec!["127.0.0.1:2379".to_string()])
                .insecure()
                .build()
                .is_ok()
        );
        assert!(
            TikvConfig {
                endpoints: Some(vec!["127.0.0.1:2379".to_string()]),
                insecure: true,
                ca_path: Some("".to_string()),
                cert_path: Some("".to_string()),
                key_path: Some("".to_string()),
            }
            .into_builder()
            .build()
            .is_ok()
        );

        // Valid TLS config with all three paths set.
        assert!(
            TikvBuilder::default()
                .endpoints(vec!["127.0.0.1:2379".to_string()])
                .ca_path("/ca.pem")
                .cert_path("/cert.pem")
                .key_path("/key.pem")
                .build()
                .is_ok()
        );
    }

    #[test]
    fn from_uri_respects_tls_and_endpoint_validation() {
        let missing_tls =
            OperatorUri::new("tikv://pd1:2379", Vec::<(String, String)>::new()).unwrap();
        let err = TikvConfig::from_uri(&missing_tls)
            .unwrap()
            .into_builder()
            .build()
            .expect_err("default !insecure without TLS paths should fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let insecure_uri = OperatorUri::new(
            "tikv://pd1:2379",
            vec![
                ("insecure".to_string(), "true".to_string()),
                ("ca_path".to_string(), "".to_string()),
            ],
        )
        .unwrap();
        assert!(
            TikvConfig::from_uri(&insecure_uri)
                .unwrap()
                .into_builder()
                .build()
                .is_ok()
        );

        let tls_uri = OperatorUri::new(
            "tikv://pd1:2379",
            vec![
                ("ca_path".to_string(), "/ca.pem".to_string()),
                ("cert_path".to_string(), "/cert.pem".to_string()),
                ("key_path".to_string(), "/key.pem".to_string()),
            ],
        )
        .unwrap();
        assert!(
            TikvConfig::from_uri(&tls_uri)
                .unwrap()
                .into_builder()
                .build()
                .is_ok()
        );
    }

    #[tokio::test]
    async fn stat_root_returns_dir_without_connecting() {
        let service = TikvBuilder::default()
            .endpoints(vec!["http://127.0.0.1:1".to_string()])
            .insecure()
            .build()
            .unwrap();

        let rp = tokio::time::timeout(
            Duration::from_secs(5),
            service.stat(&OperationContext::default(), "/", OpStat::default()),
        )
        .await
        .expect("stat root should not hang")
        .expect("stat root should succeed");
        assert_eq!(rp.into_metadata().mode(), EntryMode::DIR);
    }

    #[tokio::test]
    async fn connect_errors_distinguish_transient_and_config_invalid() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let unused_addr = listener.local_addr().unwrap();
        drop(listener);

        let ctx = OperationContext::default();

        // Unreachable PD must surface as temporary Unexpected, not permanent ConfigInvalid.
        let unreachable = TikvBuilder::default()
            .endpoints(vec![format!("http://{unused_addr}")])
            .insecure()
            .build()
            .unwrap();
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            unreachable.stat(&ctx, "probe", OpStat::default()),
        )
        .await
        .expect("connect to closed port should return quickly")
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(
            err.is_temporary(),
            "unreachable PD error should be temporary: {err:?}"
        );

        // Nonexistent TLS files must surface as permanent ConfigInvalid.
        let bad_tls = TikvBuilder::default()
            .endpoints(vec![format!("http://{unused_addr}")])
            .ca_path("/nonexistent/tikv-ca.pem")
            .cert_path("/nonexistent/tikv-cert.pem")
            .key_path("/nonexistent/tikv-key.pem")
            .build()
            .unwrap();
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            bad_tls.stat(&ctx, "probe", OpStat::default()),
        )
        .await
        .expect("TLS load failure should return quickly")
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(!err.is_temporary());

        // Duplicate PD endpoints must surface as permanent ConfigInvalid.
        let duplicate = TikvBuilder::default()
            .endpoints(vec![
                format!("http://{unused_addr}"),
                format!("http://{unused_addr}"),
            ])
            .insecure()
            .build()
            .unwrap();
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            duplicate.stat(&ctx, "probe", OpStat::default()),
        )
        .await
        .expect("duplicate endpoints should fail quickly")
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(!err.is_temporary());
    }
}
