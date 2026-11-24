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

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::Mutex;

use crate::*;

/// Value stored in memory containing both metadata and content
#[derive(Clone)]
pub struct MemoryValue {
    pub metadata: Metadata,
    pub content: Buffer,
}

#[derive(Clone)]
pub struct MemoryCore {
    pub data: Arc<Mutex<BTreeMap<String, MemoryValue>>>,
}

impl Debug for MemoryCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryCore").finish_non_exhaustive()
    }
}

impl MemoryCore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<MemoryValue>> {
        Ok(self.data.lock().unwrap().get(key).cloned())
    }

    pub fn set(&self, key: &str, value: MemoryValue) -> Result<()> {
        self.data.lock().unwrap().insert(key.to_string(), value);
        Ok(())
    }

    pub fn set_if_not_exists(&self, key: &str, value: MemoryValue) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        match data.entry(key.to_string()) {
            Entry::Vacant(entry) => entry.insert(value),
            Entry::Occupied(_) => {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "key already exists",
                ));
            }
        };
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        if (key.is_empty() || key.ends_with('/'))
            && data
                .range::<str, _>((Bound::Excluded(key), Bound::Unbounded))
                .next()
                .is_some_and(|(child, _)| child.starts_with(key))
        {
            return Err(Error::new(ErrorKind::Unexpected, "directory not empty"));
        }
        data.remove(key);
        Ok(())
    }

    pub fn delete_prefix(&self, prefix: &str) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let keys: Vec<_> = data
            .range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
            .map(|(key, _)| key)
            .take_while(|key| key.starts_with(prefix))
            .cloned()
            .collect();
        for key in keys {
            data.remove(&key);
        }
        Ok(())
    }

    /// Returns the keys starting with `prefix` in OpenDAL path order.
    ///
    /// `root_key` is the stored key for the root directory marker (`"/"`).
    /// Because the root marker is stored under `root_key` rather than
    /// `"{root_key}/"`, it sorts before keys whose relative path starts with a
    /// byte smaller than `'/'` (such as `'-'` or `'.'`). Handle `root_key`
    /// separately so `"/"` is filtered and ordered by its OpenDAL path.
    pub fn scan(
        &self,
        root_key: &str,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<Vec<String>> {
        let data = self.data.lock().unwrap();

        let include_root = prefix == root_key
            && start_after.is_none_or(|marker| "/" > marker)
            && data.contains_key(root_key);

        let abs_start_after = start_after.map(|marker| format!("{root_key}{marker}"));
        let lower = match abs_start_after.as_deref() {
            Some(marker) if marker >= prefix => Bound::Excluded(marker),
            _ if prefix == root_key => Bound::Excluded(root_key),
            _ => Bound::Included(prefix),
        };

        let mut keys: Vec<String> = data
            .range::<str, _>((lower, Bound::Unbounded))
            .map(|(key, _)| key)
            .take_while(|key| key.starts_with(prefix))
            .cloned()
            .collect();

        if include_root {
            let pos = keys
                .partition_point(|key| key.strip_prefix(root_key).unwrap_or(key.as_str()) < "/");
            keys.insert(pos, root_key.to_string());
        }

        Ok(keys)
    }
}
