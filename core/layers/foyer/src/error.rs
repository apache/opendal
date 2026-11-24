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

use foyer::Error as FoyerError;

use opendal_core::Buffer;
use opendal_core::Error;
use opendal_core::ErrorKind;

#[derive(Debug)]
pub(crate) enum FetchError {
    /// The object is not eligible for caching (for example `stat` is
    /// unsupported, the entry is not a file, or its size exceeds
    /// `size_limit`), so the reader should fall back to reading directly from
    /// the underlying service.
    BypassCache,
    /// The fetched value was committed to Foyer under `DeletedKeys`'s cache
    /// lock so Foyer's `RawFetch::handle_target` does not perform a second
    /// unsynchronized insert after `fetch_guard` drops.
    Committed,
    /// The key was invalidated or overwritten while this fetch was in flight.
    /// Carry the fetched bytes and the leader's read epoch back to waiting
    /// readers without caching them.
    Stale {
        buffer: Buffer,
        fetch_epoch: u64,
    },
    /// The fetched object's actual size was outside `size_limit` after
    /// reading. Carry the fetched bytes back to same-epoch readers without
    /// caching them.
    Uncached {
        buffer: Buffer,
        fetch_epoch: u64,
    },
    Source(Arc<Error>),
}

impl FetchError {
    pub(crate) fn from_error(err: Error) -> Self {
        Self::Source(Arc::new(err))
    }
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BypassCache => write!(f, "object is not eligible for caching"),
            Self::Committed => write!(f, "fetched data was already committed to cache"),
            Self::Stale { .. } => write!(f, "fetched data was invalidated while in flight"),
            Self::Uncached { .. } => write!(f, "fetched data size is outside size limit"),
            Self::Source(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for FetchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

fn clone_shared_error(err: &Arc<Error>) -> Error {
    Error::new(err.kind(), err.message())
        .with_temporary(err.is_temporary())
        .with_persistent(err.is_persistent())
        .set_source(Arc::clone(err))
}

pub(crate) fn extract_err(e: FoyerError) -> Error {
    if let Some(fetch_err) = e.downcast_ref::<FetchError>() {
        return match fetch_err {
            FetchError::BypassCache => {
                Error::new(ErrorKind::Unexpected, "object is not eligible for caching")
            }
            FetchError::Committed => Error::new(
                ErrorKind::Unexpected,
                "fetched data was already committed to cache",
            ),
            FetchError::Stale { .. } => Error::new(
                ErrorKind::Unexpected,
                "fetched data was invalidated while in flight",
            ),
            FetchError::Uncached { .. } => Error::new(
                ErrorKind::Unexpected,
                "fetched data size is outside size limit",
            ),
            FetchError::Source(err) => clone_shared_error(err),
        };
    }
    if let Some(src) = e.downcast_ref::<Error>() {
        let kind = src.kind();
        let message = src.message().to_string();
        let temporary = src.is_temporary();
        let persistent = src.is_persistent();
        return Error::new(kind, message)
            .with_temporary(temporary)
            .with_persistent(persistent)
            .set_source(e);
    }
    Error::new(ErrorKind::Unexpected, "foyer operation failed").set_source(e)
}
