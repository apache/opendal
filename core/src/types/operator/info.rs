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

use crate::raw::*;
use crate::*;

/// Metadata for operator, users can use this metadata to get information of operator.
#[derive(Clone, Debug, Default)]
pub struct OperatorInfo(Arc<AccessorInfo>);

impl OperatorInfo {
    pub(super) fn new(acc: Arc<AccessorInfo>) -> Self {
        OperatorInfo(acc)
    }

    /// [`Scheme`] of operator.
    pub fn scheme(&self) -> Scheme {
        self.0.scheme()
    }

    /// Root of operator, will be in format like `/path/to/dir/`
    pub fn root(&self) -> String {
        self.0.root().to_string()
    }

    /// Name of backend, could be empty if underlying backend doesn't have namespace concept.
    ///
    /// For example:
    ///
    /// - name for `s3` => bucket name
    /// - name for `azblob` => container name
    pub fn name(&self) -> String {
        self.0.name().to_string()
    }

    /// Get [`Full Capability`] of operator.
    pub fn full_capability(&self) -> Capability {
        self.0.full_capability()
    }

    /// Get [`Native Capability`] of operator.
    pub fn native_capability(&self) -> Capability {
        self.0.native_capability()
    }
}
