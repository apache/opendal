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

use std::io;
use std::path::Path;

use fuse3::path::Session;
use fuse3::raw::MountHandle;
use fuse3::MountOptions;
use opendal::Operator;

/// Ofs fuse filesystem mounter.
#[derive(Debug, Clone)]
pub struct Fuse {
    mount_options: MountOptions,
    uid: Option<u32>,
    gid: Option<u32>,
}

impl Fuse {
    pub fn new() -> Self {
        Fuse::default()
    }

    /// Set fuse filesystem mount user_id, default is current uid.
    pub fn uid(mut self, uid: u32) -> Self {
        self.uid.replace(uid);
        self.mount_options.uid(uid);
        self
    }

    /// Set fuse filesystem mount group_id, default is current gid.
    pub fn gid(mut self, gid: u32) -> Self {
        self.gid.replace(gid);
        self.mount_options.gid(gid);
        self
    }

    /// Set fuse filesystem name, default is __OpenDAL Filesystem__.
    pub fn fs_name(mut self, name: impl Into<String>) -> Self {
        self.mount_options.fs_name(name);
        self
    }

    /// Set fuse filesystem `allow_root` mount option, default is disable.
    pub fn allow_root(mut self, allow_root: bool) -> Self {
        self.mount_options.allow_root(allow_root);
        self
    }

    /// Set fuse filesystem allow_other mount option, default is disable.
    pub fn allow_other(mut self, allow_other: bool) -> Self {
        self.mount_options.allow_other(allow_other);
        self
    }

    /// Set fuse filesystem `ro` mount option, default is disable.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.mount_options.read_only(read_only);
        self
    }

    /// Allow fuse filesystem mount on a non-empty directory, default is not allowed.
    pub fn allow_non_empty(mut self, allow_non_empty: bool) -> Self {
        self.mount_options.nonempty(allow_non_empty);
        self
    }

    /// Mount the filesystem with root permission.
    pub async fn mount(
        self,
        mount_point: impl AsRef<Path>,
        op: Operator,
    ) -> io::Result<MountHandle> {
        let adapter = fuse3_opendal::Filesystem::new(
            op,
            self.uid.unwrap_or_else(|| nix::unistd::getuid().into()),
            self.gid.unwrap_or_else(|| nix::unistd::getgid().into()),
        );
        Session::new(self.mount_options)
            .mount(adapter, mount_point)
            .await
    }

    /// Mount the filesystem without root permission.
    pub async fn mount_with_unprivileged(
        self,
        mount_point: impl AsRef<Path>,
        op: Operator,
    ) -> io::Result<MountHandle> {
        log::warn!("unprivileged mount may not detect external unmount, tracking issue: https://github.com/Sherlock-Holo/fuse3/issues/72");

        let adapter = fuse3_opendal::Filesystem::new(
            op,
            self.uid.unwrap_or_else(|| nix::unistd::getuid().into()),
            self.gid.unwrap_or_else(|| nix::unistd::getgid().into()),
        );
        Session::new(self.mount_options)
            .mount_with_unprivileged(adapter, mount_point)
            .await
    }
}

impl Default for Fuse {
    fn default() -> Self {
        let mut mount_options = MountOptions::default();
        mount_options.fs_name("ofs").no_open_dir_support(true);

        Self {
            mount_options,
            uid: None,
            gid: None,
        }
    }
}
