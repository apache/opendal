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

use super::OPFS_SCHEME;
use opendal_core::raw::*;
use opendal_core::*;

#[derive(Debug, Clone)]
pub struct OpfsCore {
    pub root: String,
    pub info: Arc<AccessorInfo>,
}

impl OpfsCore {
    pub fn new(root: String) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(OPFS_SCHEME);
        info.set_name("opfs");
        info.set_root(&root);
        info.set_native_capability(Capability {
            stat: true,

            read: true,

            list: true,

            create_dir: true,

            write: true,
            write_can_empty: true,
            write_can_multi: true,

            delete: true,

            ..Default::default()
        });

        Self {
            root,
            info: Arc::new(info),
        }
    }
}
