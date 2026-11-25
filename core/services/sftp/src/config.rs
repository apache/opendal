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

use opendal_core::raw::SignedDuration;
use serde::Deserialize;
use serde::Serialize;

use super::backend::SftpBuilder;

/// Config for Sftp Service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SftpConfig {
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// root of this backend
    pub root: Option<String>,
    /// user of this backend
    pub user: Option<String>,
    /// key of this backend
    pub key: Option<String>,
    /// known_hosts_strategy of this backend
    pub known_hosts_strategy: Option<String>,
    /// Time limit for waiting for an SFTP connection pool slot. Defaults to 10 seconds. Zero disables the limit.
    pub acquire_timeout: Option<SignedDuration>,
    /// Time limit for each SFTP connection setup stage and each pooled connection health check. Defaults to 10 seconds. Zero disables the limit.
    pub connect_timeout: Option<SignedDuration>,
    /// Directory for the SSH control socket.
    ///
    /// The SSH client creates a temporary directory holding the control socket here.
    /// Defaults to `$XDG_STATE_HOME` or `$HOME/.local/state`, falling back to the current directory.
    pub control_directory: Option<String>,
    /// Deprecated: SFTP copy capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "SFTP copy capability is enabled by default and this option is no longer needed."
    )]
    pub enable_copy: bool,
}

impl Debug for SftpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpConfig")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl opendal_core::Configurator for SftpConfig {
    type Builder = SftpBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();
        if let Some(authority) = uri.authority() {
            map.insert("endpoint".to_string(), format!("ssh://{authority}"));
        }

        if let Some(username) = uri.username() {
            map.entry("user".to_string())
                .or_insert_with(|| username.to_string());
        }

        if let Some(root) = uri.root() {
            map.insert("root".to_string(), root.to_string());
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        SftpBuilder {
            config: self,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;

    #[test]
    fn from_uri_sets_endpoint_and_root() {
        let uri = OperatorUri::new(
            "sftp://sftp.example.com/home/alice",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = SftpConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("ssh://sftp.example.com"));
        assert_eq!(cfg.root.as_deref(), Some("home/alice"));
    }

    #[test]
    fn from_uri_preserves_port_and_username() {
        let uri = OperatorUri::new(
            "sftp://foo@127.0.0.1:2222/upload/data",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = SftpConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("ssh://127.0.0.1:2222"));
        assert_eq!(cfg.user.as_deref(), Some("foo"));
        assert_eq!(cfg.root.as_deref(), Some("upload/data"));

        let builder = openssh::SessionBuilder::default();
        let (resolved, destination) = builder.resolve(cfg.endpoint.as_deref().unwrap());
        assert_eq!(resolved.get_port(), Some("2222"));
        assert_eq!(destination, "127.0.0.1");
    }

    #[test]
    fn from_uri_explicit_user_option_overrides_uri_username() {
        let uri = OperatorUri::new(
            "sftp://bob@127.0.0.1:2222/upload",
            [("user".to_string(), "alice".to_string())],
        )
        .unwrap();

        let cfg = SftpConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("ssh://127.0.0.1:2222"));
        assert_eq!(cfg.user.as_deref(), Some("alice"));
    }

    #[test]
    fn from_uri_applies_timeout_overrides() {
        let uri = OperatorUri::new(
            "sftp://host",
            [
                ("acquire_timeout".to_string(), "0s".to_string()),
                ("connect_timeout".to_string(), "500ms".to_string()),
            ],
        )
        .unwrap();

        let cfg = SftpConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.acquire_timeout, Some(SignedDuration::ZERO));
        assert_eq!(cfg.connect_timeout, Some(SignedDuration::from_millis(500)));
    }

    #[test]
    fn config_rejects_malformed_timeouts() {
        use opendal_core::ErrorKind;

        for field in ["acquire_timeout", "connect_timeout"] {
            let error = SftpConfig::from_iter([(field.to_string(), "invalid".to_string())])
                .expect_err("malformed duration");
            assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
        }
    }

    #[test]
    fn builder_rejects_negative_configured_timeouts() {
        use opendal_core::{Builder, ErrorKind};

        for field in ["acquire_timeout", "connect_timeout"] {
            let pairs = [("endpoint", "host"), (field, "-1s")];
            let config =
                SftpConfig::from_iter(pairs.map(|(key, value)| (key.into(), value.into())))
                    .unwrap();
            let error = config.into_builder().build().expect_err("negative timeout");
            assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
            assert!(error.to_string().contains(field));
        }
    }

    #[test]
    fn from_uri_applies_connection_overrides() {
        let uri = OperatorUri::new(
            "sftp://host",
            vec![
                ("user".to_string(), "alice".to_string()),
                ("key".to_string(), "/home/alice/.ssh/id_rsa".to_string()),
                ("known_hosts_strategy".to_string(), "accept".to_string()),
            ],
        )
        .unwrap();

        let cfg = SftpConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("ssh://host"));
        assert_eq!(cfg.user.as_deref(), Some("alice"));
        assert_eq!(cfg.key.as_deref(), Some("/home/alice/.ssh/id_rsa"));
        assert_eq!(cfg.known_hosts_strategy.as_deref(), Some("accept"));
    }

    #[test]
    fn control_directory_from_map_builder_and_uri() {
        let cfg = SftpConfig::from_iter([(
            "control_directory".to_string(),
            "/tmp/sftp-control".to_string(),
        )])
        .unwrap();
        assert_eq!(cfg.control_directory.as_deref(), Some("/tmp/sftp-control"));

        let builder = SftpBuilder::default().control_directory("/tmp/sftp-control");
        assert_eq!(
            builder.config.control_directory.as_deref(),
            Some("/tmp/sftp-control")
        );
        assert_eq!(
            SftpBuilder::default()
                .control_directory("")
                .config
                .control_directory,
            None
        );

        let uri = OperatorUri::new(
            "sftp://host",
            [(
                "control_directory".to_string(),
                "/tmp/sftp-control".to_string(),
            )],
        )
        .unwrap();
        let cfg = SftpConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.control_directory.as_deref(), Some("/tmp/sftp-control"));
    }

    #[test]
    fn builder_builds_with_control_directory() {
        use opendal_core::Builder;

        let builder = SftpBuilder::default()
            .endpoint("host")
            .control_directory("/tmp/sftp-control");
        assert!(builder.build().is_ok());
    }

    #[test]
    fn capability_does_not_advertise_list_with_limit() {
        use opendal_core::Builder;
        use opendal_core::raw::Service;

        let service = SftpBuilder::default().endpoint("host").build().unwrap();
        assert!(!service.capability().list_with_limit);

        let op = opendal_core::Operator::new(SftpBuilder::default().endpoint("host")).unwrap();
        assert!(!op.info().capability().list_with_limit);
    }
}
