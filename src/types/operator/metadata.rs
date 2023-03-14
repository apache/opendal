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

use crate::raw::*;
use crate::*;

/// Metadata for operator, users can use this metadata to get information of operator.
#[derive(Clone, Debug, Default)]
pub struct OperatorInfo(AccessorInfo);

impl OperatorInfo {
    pub(super) fn new(acc: AccessorInfo) -> Self {
        OperatorInfo(acc)
    }

    /// [`Scheme`] of operator.
    pub fn scheme(&self) -> Scheme {
        self.0.scheme()
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

    /// Check if current backend supports [`Accessor::read`] or not.
    pub fn can_read(&self) -> bool {
        self.0.capabilities().contains(AccessorCapability::Read)
    }

    /// Check if current backend supports [`Accessor::write`] or not.
    pub fn can_write(&self) -> bool {
        self.0.capabilities().contains(AccessorCapability::Write)
    }

    /// Check if current backend supports [`Accessor::list`] or not.
    pub fn can_list(&self) -> bool {
        self.0.capabilities().contains(AccessorCapability::List)
    }

    /// Check if current backend supports [`Accessor::scan`] or not.
    pub fn can_scan(&self) -> bool {
        self.0.capabilities().contains(AccessorCapability::Scan)
    }

    /// Check if current backend supports [`Accessor::presign`] or not.
    pub fn can_presign(&self) -> bool {
        self.0.capabilities().contains(AccessorCapability::Presign)
    }

    /// Check if current backend supports batch operations or not.
    pub fn can_batch(&self) -> bool {
        self.0.capabilities().contains(AccessorCapability::Batch)
    }

    /// Check if current backend supports blocking operations or not.
    pub fn can_blocking(&self) -> bool {
        self.0.capabilities().contains(AccessorCapability::Blocking)
    }
}
