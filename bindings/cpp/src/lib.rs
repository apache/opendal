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

use anyhow::Result;
use opendal as od;
use std::str::FromStr;

#[cxx::bridge(namespace = "opendal::ffi")]
mod ffi {
    struct HashMapValue {
        key: String,
        value: String,
    }

    struct Metadata {
        // tag layout: (8 bits flagset)
        // 0-1: mode, 2: has_cache_control, 3: has_content_disposition, 4: has_content_md5,
        // 5: has_content_type, 6: has_etag, 7: has_last_modified
        //
        // mode enum: (2 bits)
        // 1: file, 2: dir, 0,3: unknown
        tag: u8,
        content_length: u64,
        cache_control: String,
        content_disposition: String,
        content_md5: String,
        content_type: String,
        etag: String,
        last_modified: String,
    }

    struct Entry {
        path: String,
    }

    extern "Rust" {
        type Operator;

        fn new_operator(scheme: &str, configs: Vec<HashMapValue>) -> Result<Box<Operator>>;
        fn read(self: &Operator, path: &str) -> Result<Vec<u8>>;
        fn write(self: &Operator, path: &str, bs: &'static [u8]) -> Result<()>;
        fn is_exist(self: &Operator, path: &str) -> Result<bool>;
        fn create_dir(self: &Operator, path: &str) -> Result<()>;
        fn copy(self: &Operator, src: &str, dst: &str) -> Result<()>;
        fn rename(self: &Operator, src: &str, dst: &str) -> Result<()>;
        fn remove(self: &Operator, path: &str) -> Result<()>;
        fn stat(self: &Operator, path: &str) -> Result<Metadata>;
        fn list(self: &Operator, path: &str) -> Result<Vec<Entry>>;
    }
}

struct Operator(od::BlockingOperator);

fn new_operator(scheme: &str, configs: Vec<ffi::HashMapValue>) -> Result<Box<Operator>> {
    let scheme = od::Scheme::from_str(scheme)?;

    let map = configs
        .into_iter()
        .map(|value| (value.key, value.value))
        .collect();

    let op = Box::new(Operator(od::Operator::via_map(scheme, map)?.blocking()));

    Ok(op)
}

impl Operator {
    fn read(&self, path: &str) -> Result<Vec<u8>> {
        Ok(self.0.read(path)?)
    }

    // To avoid copying the bytes, we use &'static [u8] here.
    //
    // Safety: The bytes created from bs will be dropped after the function call.
    // So it's safe to declare its lifetime as 'static.
    fn write(&self, path: &str, bs: &'static [u8]) -> Result<()> {
        Ok(self.0.write(path, bs)?)
    }

    fn is_exist(&self, path: &str) -> Result<bool> {
        Ok(self.0.is_exist(path)?)
    }

    fn create_dir(&self, path: &str) -> Result<()> {
        Ok(self.0.create_dir(path)?)
    }

    fn copy(&self, src: &str, dst: &str) -> Result<()> {
        Ok(self.0.copy(src, dst)?)
    }

    fn rename(&self, src: &str, dst: &str) -> Result<()> {
        Ok(self.0.rename(src, dst)?)
    }

    // We can't name it to delete because it's a keyword in C++
    fn remove(&self, path: &str) -> Result<()> {
        Ok(self.0.delete(path)?)
    }

    fn stat(&self, path: &str) -> Result<ffi::Metadata> {
        Ok(self.0.stat(path)?.into())
    }

    fn list(&self, path: &str) -> Result<Vec<ffi::Entry>> {
        Ok(self.0.list(path)?.into_iter().map(Into::into).collect())
    }
}

impl From<od::Metadata> for ffi::Metadata {
    fn from(meta: od::Metadata) -> Self {
        let mut tag = 0u8;

        match meta.mode() {
            od::EntryMode::FILE => tag |= 0b0000_0001,
            od::EntryMode::DIR => tag |= 0b0000_0010,
            _ => {}
        }

        let content_length = meta.content_length();

        let cache_control = match meta.cache_control() {
            Some(v) => {
                tag |= 0b0000_0100;
                v.to_owned()
            }
            None => String::new(),
        };

        let content_disposition = match meta.content_disposition() {
            Some(v) => {
                tag |= 0b0000_1000;
                v.to_owned()
            }
            None => String::new(),
        };

        let content_md5 = match meta.content_md5() {
            Some(v) => {
                tag |= 0b0001_0000;
                v.to_owned()
            }
            None => String::new(),
        };

        let content_type = match meta.content_type() {
            Some(v) => {
                tag |= 0b0010_0000;
                v.to_owned()
            }
            None => String::new(),
        };

        let etag = match meta.etag() {
            Some(v) => {
                tag |= 0b0100_0000;
                v.to_owned()
            }
            None => String::new(),
        };

        let last_modified = match meta.last_modified() {
            Some(v) => {
                tag |= 0b1000_0000;
                v.to_rfc3339_opts(chrono::SecondsFormat::Nanos, false)
            }
            None => String::new(),
        };

        Self {
            tag,
            content_length,
            cache_control,
            content_disposition,
            content_md5,
            content_type,
            etag,
            last_modified,
        }
    }
}

impl From<od::Entry> for ffi::Entry {
    fn from(entry: od::Entry) -> Self {
        let (path, _) = entry.into_parts();
        Self { path }
    }
}
