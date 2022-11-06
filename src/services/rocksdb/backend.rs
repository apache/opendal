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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;
use std::vec::IntoIter;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::Future;
use futures::Stream;
use pin_project::pin_project;
use rocksdb::TransactionDB;

use crate::adapters::kv;
use crate::error::new_other_backend_error;
use crate::Accessor;
use crate::AccessorCapability;
use crate::Scheme;

const SCAN_LIMIT: usize = 100;

/// Rocksdb backend builder
#[derive(Clone, Default, Debug)]
pub struct Builder {
    /// The path to the rocksdb database
    path: Option<PathBuf>,
    /// the working directory of the Redis service. Can be "/path/to/dir"
    ///
    /// default is "/"
    root: Option<String>,
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();
        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "path" => builder.path(v),
                _ => continue,
            };
        }
        builder
    }

    /// Set the path to the rocksdb database
    pub fn path<P: AsRef<Path>>(&mut self, path: P) -> &mut Self {
        self.path = Some(path.as_ref().into());
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }

    /// Consumes the builder and returns a `Rocksdb` instance.
    pub fn build(&mut self) -> Result<impl Accessor> {
        let path = self.path.take().ok_or_else(|| {
            new_other_backend_error(
                HashMap::from([("path".into(), "".into())]),
                anyhow!("path is required but not set"),
            )
        })?;
        let db = TransactionDB::open_default(&path).map_err(|e| {
            new_other_backend_error(
                HashMap::from([("path".into(), path.to_string_lossy().into_owned())]),
                anyhow!("failed to open the database: {:?}", e),
            )
        })?;

        Ok(Backend::new(Adapter { db: Arc::new(db) }))
    }
}

/// Backend for rocksdb services.
pub type Backend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    db: Arc<TransactionDB>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("path", &self.db.path());
        ds.finish()
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Rocksdb,
            &self.db.path().to_string_lossy(),
            AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
        )
    }

    async fn next_id(&self) -> Result<u64> {
        let txn = self.db.transaction();
        match txn.get("next_id").map_err(new_rocksdb_error)? {
            Some(v) => {
                let id = String::from_utf8_lossy(&v)
                    .parse::<u64>()
                    .map_err(new_rocksdb_error)?;
                txn.put("next_id", (id + 1).to_string().as_bytes())
                    .map_err(new_rocksdb_error)?;
                txn.commit().map_err(new_rocksdb_error)?;
                Ok(id + 1)
            }
            None => {
                txn.put("next_id", "1".as_bytes())
                    .map_err(new_rocksdb_error)?;
                txn.commit().map_err(new_rocksdb_error)?;
                Ok(1)
            }
        }
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key).map_err(new_rocksdb_error)
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value).map_err(new_rocksdb_error)
    }

    async fn scan(&self, prefix: &[u8]) -> Result<kv::KeyStreamer> {
        Ok(Box::new(KeyStream::new(self.db.clone(), prefix)))
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key).map_err(new_rocksdb_error)
    }
}

type ScanKeysResult = Result<(Option<Vec<u8>>, Vec<Vec<u8>>)>; // (cursor, keys)

#[pin_project]
struct KeyStream {
    db: Arc<TransactionDB>,
    prefix: Vec<u8>,
    cursor: Option<Vec<u8>>,
    keys: IntoIter<Vec<u8>>,
    fut: Option<BoxFuture<'static, ScanKeysResult>>,
}

impl KeyStream {
    fn new(db: Arc<TransactionDB>, prefix: &[u8]) -> Self {
        Self {
            db,
            prefix: prefix.to_vec(),
            cursor: Some(prefix.to_vec()),
            keys: vec![].into_iter(),
            fut: None,
        }
    }
}

impl Stream for KeyStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            if let Some(key) = this.keys.next() {
                return Poll::Ready(Some(Ok(key)));
            }

            match this.fut {
                None => match this.cursor.take() {
                    None => return Poll::Ready(None),
                    Some(cursor) => {
                        let db = this.db.clone();
                        let prefix = this.prefix.clone();
                        let fut = async move {
                            scan_keys(db.as_ref(), &prefix, &cursor, SCAN_LIMIT)
                                .map_err(new_rocksdb_error)
                        };
                        *this.fut = Some(Box::pin(fut));
                        continue;
                    }
                },
                Some(fut) => {
                    let (cursor, keys) = ready!(Pin::new(fut).poll(cx))?;
                    *this.cursor = cursor;
                    *this.fut = None;
                    *this.keys = keys.into_iter();
                    continue;
                }
            }
        }
    }
}

fn scan_keys(db: &TransactionDB, prefix: &[u8], cursor: &[u8], limit: usize) -> ScanKeysResult {
    let keys: Result<Vec<_>> = db
        .prefix_iterator(cursor)
        .take(limit)
        .map(|kv| kv.map(|(k, _)| k.to_vec()).map_err(new_rocksdb_error))
        .take_while(|k| match k {
            Ok(k) => k.starts_with(prefix),
            Err(_) => true,
        })
        .collect();
    keys.map(|keys| match &keys[..] {
        [] => (None, keys),
        [.., last] => {
            if keys.len() < limit {
                (None, keys)
            } else {
                let cursor = last.iter().cloned().chain(Some(0)).collect();
                (Some(cursor), keys)
            }
        }
    })
}

fn new_rocksdb_error(err: impl std::error::Error) -> Error {
    Error::new(ErrorKind::Other, anyhow!("rocksdb: {err:?}"))
}
