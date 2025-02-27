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

use magnus::class;
use magnus::method;
use magnus::prelude::*;
use magnus::Error;
use magnus::RModule;

use crate::capability::Capability;
use crate::*;

/// Metadata for operator, users can use this metadata to get information of operator.
#[magnus::wrap(class = "OpenDAL::OperatorInfo", free_immediately, size)]
pub struct OperatorInfo(pub ocore::OperatorInfo);

impl OperatorInfo {
    /// Scheme string of operator.
    pub fn scheme(&self) -> &str {
        self.0.scheme().into()
    }

    /// Root of operator, will be in format like `/path/to/dir/`
    pub fn root(&self) -> &str {
        self.0.root()
    }

    /// Name of backend, could be empty if underlying backend doesn't have namespace concept.
    ///
    /// For example:
    ///
    /// - name for `s3` => bucket name
    /// - name for `azblob` => container name
    pub fn name(&self) -> &str {
        self.0.name()
    }

    /// Get [`Full Capability`] of operator.
    pub fn full_capability(&self) -> Capability {
        Capability::new(self.0.full_capability())
    }

    /// Get [`Native Capability`] of operator.
    pub fn native_capability(&self) -> Capability {
        Capability::new(self.0.native_capability())
    }
}

pub fn include(gem_module: &RModule) -> Result<(), Error> {
    let class = gem_module.define_class("OperatorInfo", class::object())?;
    class.define_method("scheme", method!(OperatorInfo::scheme, 0))?;
    class.define_method("root", method!(OperatorInfo::root, 0))?;
    class.define_method("name", method!(OperatorInfo::name, 0))?;
    class.define_method("full_capability", method!(OperatorInfo::full_capability, 0))?;
    class.define_method(
        "native_capability",
        method!(OperatorInfo::native_capability, 0),
    )?;

    Ok(())
}
