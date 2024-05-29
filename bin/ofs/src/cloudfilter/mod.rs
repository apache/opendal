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

mod filter_impl;

use filter_impl::FilterImpl;
use opendal::Operator;
pub use wincs::ProtectionMode;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use widestring::U16String;
use wincs::{
    Connection, HydrationType, PopulationType, Registration, SecurityId, Session, SyncRootId,
    SyncRootIdBuilder,
};

#[derive(Debug, Default)]
pub struct SyncRootRegistration {
    display_name: Option<String>,
    account_name: Option<String>,
    protection_mode: Option<ProtectionMode>,
    icon: Option<(PathBuf, u16)>,
    blob: Option<Box<[u8]>>,
}

impl SyncRootRegistration {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name.replace(display_name.into());
        self
    }

    pub fn account_name(mut self, account_name: impl Into<String>) -> Self {
        self.account_name.replace(account_name.into());
        self
    }

    pub fn icon(mut self, path: impl Into<PathBuf>, index: u16) -> Self {
        self.icon.replace((path.into(), index));
        self
    }

    pub fn protection_mode(mut self, protection_mode: ProtectionMode) -> Self {
        self.protection_mode.replace(protection_mode);
        self
    }

    pub fn blob(mut self, blob: impl Into<Box<[u8]>>) -> Self {
        self.blob.replace(blob.into());
        self
    }

    pub fn register(
        self,
        provider_name: impl Into<String>,
        version: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<SyncRoot> {
        let mut builder = SyncRootIdBuilder::new(U16String::from_str(&provider_name.into()))
            .user_security_id(SecurityId::current_user().context("Failed to get security id")?);
        if let Some(account_name) = self.account_name {
            builder = builder.account_name(U16String::from_str(&account_name));
        }
        let sync_root_id = builder.build();

        let version_utf16 = U16String::from_str(&version.into());

        let mut registration = Registration::from_sync_root_id(&sync_root_id)
            .hydration_type(HydrationType::Full)
            .population_type(PopulationType::Full)
            .version(&version_utf16);
        let display_name_utf16 = self.display_name.map(|s| U16String::from_str(&s));
        if let Some(display_name) = display_name_utf16.as_ref() {
            registration = registration.display_name(display_name);
        }
        if let Some(protection_mode) = self.protection_mode {
            registration = registration.protection_mode(protection_mode);
        }
        let icon_utf16 = self
            .icon
            .map(|(p, i)| (U16String::from_os_str(p.as_os_str()), i));
        if let Some((path, index)) = icon_utf16 {
            registration = registration.icon(path, index);
        }
        if let Some(blob) = self.blob.as_ref() {
            registration = registration.blob(blob);
        }
        registration
            .register(&path)
            .map(|_| SyncRoot {
                sync_root_id,
                root: path.as_ref().to_path_buf(),
            })
            .context("Failed to register sync root")
    }
}

pub struct SyncRoot {
    sync_root_id: SyncRootId,
    root: PathBuf,
}

impl SyncRoot {
    pub fn unregister(self) -> Result<()> {
        self.sync_root_id
            .unregister()
            .context("Failed to unregister sync root")
    }
}

impl TryFrom<&Path> for SyncRoot {
    type Error = anyhow::Error;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let sync_root_id = SyncRootId::from_path(path).context("Failed to get sync root id")?;
        Ok(Self {
            sync_root_id,
            root: path.to_path_buf(),
        })
    }
}

#[derive(Default)]
pub struct CloudFilter {
    connection: Option<Connection<Arc<FilterImpl>>>,
}

impl CloudFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn connect(&mut self, sync_root: &SyncRoot, op: Operator) -> Result<()> {
        let connection = Session::new()
            .connect(&sync_root.root, FilterImpl::new(op))
            .context("Failed to connect")?;
        self.connection.replace(connection);

        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<()> {
        if let Some(connection) = self.connection.take() {
            connection.disconnect().context("Failed to disconnect")?;
        }
        Ok(())
    }
}

impl Drop for CloudFilter {
    fn drop(&mut self) {
        self.disconnect().expect("disconnected");
    }
}
