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

use futures::StreamExt;

use crate::*;

/// BlockingLister is designed to list entries at given path in a blocking
/// manner.
///
/// Users can construct Lister by [`blocking::Operator::lister`] or [`blocking::Operator::lister_with`].
///
/// - Lister implements `Iterator<Item = Result<Entry>>`.
/// - Lister will return `None` if there is no more entries or error has been returned.
pub struct BlockingLister {
    handle: tokio::runtime::Handle,
    lister: Lister,
}

/// # Safety
///
/// BlockingLister will only be accessed by `&mut Self`
unsafe impl Sync for BlockingLister {}

impl BlockingLister {
    /// Create a new lister.
    pub(crate) fn new(handle: tokio::runtime::Handle, lister: Lister) -> Self {
        Self { handle, lister }
    }
}

impl Iterator for BlockingLister {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.handle.block_on(self.lister.next())
    }
}
