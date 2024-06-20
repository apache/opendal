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

use opendal::Operator;

use std::path::{Path, PathBuf};

use anyhow::Result;

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
        unimplemented!()
    }
}

pub struct SyncRoot {
    root: PathBuf,
}

impl SyncRoot {
    pub fn unregister(self) -> Result<()> {
        unimplemented!()
    }
}

impl TryFrom<&Path> for SyncRoot {
    type Error = anyhow::Error;

    fn try_from(_path: &Path) -> Result<Self, Self::Error> {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct CloudFilter {}

impl CloudFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn connect(&mut self, sync_root: &SyncRoot, op: Operator) -> Result<()> {
        unimplemented!()
    }

    pub fn disconnect(&mut self) -> Result<()> {
        unimplemented!()
    }
}

impl Drop for CloudFilter {
    fn drop(&mut self) {
        self.disconnect().expect("disconnected");
    }
}

#[derive(Debug)]
pub enum ProtectionMode {
    None,
}
