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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use rocksdb::DB;

use crate::*;

#[derive(Clone)]
pub struct RocksdbCore {
    pub db: Arc<DB>,
}

impl Debug for RocksdbCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("RocksdbCore");
        ds.field("path", &self.db.path());
        ds.finish()
    }
}

impl RocksdbCore {
    pub fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let result = self.db.get(path).map_err(parse_rocksdb_error)?;
        Ok(result.map(Buffer::from))
    }

    pub fn set(&self, path: &str, value: Buffer) -> Result<()> {
        self.db
            .put(path, value.to_vec())
            .map_err(parse_rocksdb_error)
    }

    pub fn delete(&self, path: &str) -> Result<()> {
        self.db.delete(path).map_err(parse_rocksdb_error)
    }

    pub fn list(&self, path: &str) -> Result<Vec<String>> {
        let it = self.db.prefix_iterator(path).map(|r| r.map(|(k, _)| k));
        let mut res = Vec::default();

        for key in it {
            let key = key.map_err(parse_rocksdb_error)?;
            let key = String::from_utf8_lossy(&key);
            if !key.starts_with(path) {
                break;
            }
            res.push(key.to_string());
        }

        Ok(res)
    }
}

fn parse_rocksdb_error(e: rocksdb::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "got rocksdb error").set_source(e)
}
