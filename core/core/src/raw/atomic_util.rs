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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// AtomicContentLength is a wrapper of AtomicU64 that used to store content length.
///
/// It provides a way to store and load content length in atomic way, so caller don't need to
/// use `Mutex` or `RwLock` to protect the content length.
///
/// We use value `u64::MAX` to represent unknown size, it's impossible for us to
/// handle a file that has `u64::MAX` bytes.
#[derive(Debug)]
pub struct AtomicContentLength(AtomicU64);

impl Default for AtomicContentLength {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicContentLength {
    /// Create a new AtomicContentLength.
    pub const fn new() -> Self {
        Self(AtomicU64::new(u64::MAX))
    }

    /// Load content length from AtomicU64.
    #[inline]
    pub fn load(&self) -> Option<u64> {
        match self.0.load(Ordering::Relaxed) {
            u64::MAX => None,
            v => Some(v),
        }
    }

    /// Store content length to AtomicU64.
    #[inline]
    pub fn store(&self, v: u64) {
        self.0.store(v, Ordering::Relaxed)
    }
}
