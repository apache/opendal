// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::io::Result;
use std::ops::Bound::Excluded;
use std::ops::Bound::Included;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::vec::IntoIter;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::adapters::kv;
use crate::adapters::kv::next_prefix;
use crate::Scheme;

/// Builder for memory backend
#[derive(Default)]
pub struct Builder {}

impl Builder {
    /// Consume builder to build a memory backend.
    pub fn build(&mut self) -> Result<Backend> {
        let adapter = Adapter {
            inner: Arc::new(Mutex::new(BTreeMap::default())),
            next_id: Arc::new(AtomicU64::new(1)),
        };

        Ok(Backend::new(adapter))
    }
}

/// Backend is used to serve `Accessor` support in memory.
pub type Backend = kv::Backend<Adapter>;

#[derive(Debug, Clone)]
pub struct Adapter {
    inner: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    next_id: Arc<AtomicU64>,
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(Scheme::Memory, &format!("{:?}", &self.inner as *const _))
    }

    async fn next_id(&self) -> Result<u64> {
        Ok(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.inner.lock().get(key) {
            None => Ok(None),
            Some(bs) => Ok(Some(bs.to_vec())),
        }
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.lock().insert(key.to_vec(), value.to_vec());

        Ok(())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<kv::KeyStreamer> {
        let map = self.inner.lock();
        let iter = map.range((Included(prefix.to_vec()), Excluded(next_prefix(prefix))));

        Ok(Box::new(KeyStream {
            keys: iter
                .map(|(k, _)| k.to_vec())
                .collect::<Vec<_>>()
                .into_iter(),
        }))
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.lock().remove(key);

        Ok(())
    }
}

struct KeyStream {
    keys: IntoIter<Vec<u8>>,
}

impl futures::Stream for KeyStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.keys.next() {
            None => Poll::Ready(None),
            Some(v) => Poll::Ready(Some(Ok(v))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Accessor;

    #[test]
    fn test_accessor_metadata_name() {
        let b1 = Builder::default().build().unwrap();
        assert_eq!(b1.metadata().name(), b1.metadata().name());

        let b2 = Builder::default().build().unwrap();
        assert_ne!(b1.metadata().name(), b2.metadata().name())
    }
}
