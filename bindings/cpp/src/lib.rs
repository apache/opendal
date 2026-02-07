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

#[cfg(feature = "async")]
mod r#async;
mod lister;
mod reader;
mod types;

use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::Result;
use lister::Lister;
use opendal as od;
use reader::Reader;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

#[cxx::bridge(namespace = "opendal::ffi")]
mod ffi {
    struct HashMapValue {
        key: String,
        value: String,
    }

    #[cxx_name = "SeekDir"]
    enum SeekFrom {
        Start = 0,
        Current = 1,
        End = 2,
    }

    enum EntryMode {
        File = 1,
        Dir = 2,
        Unknown = 0,
    }

    struct OptionalString {
        has_value: bool,
        value: String,
    }

    struct OptionalBool {
        has_value: bool,
        value: bool,
    }

    struct OptionalEntry {
        has_value: bool,
        value: Entry,
    }

    struct Capability {
        stat: bool,
        stat_with_if_match: bool,
        stat_with_if_none_match: bool,
        read: bool,
        read_with_if_match: bool,
        read_with_if_none_match: bool,
        read_with_override_cache_control: bool,
        read_with_override_content_disposition: bool,
        read_with_override_content_type: bool,
        write: bool,
        write_can_multi: bool,
        write_can_empty: bool,
        write_can_append: bool,
        write_with_content_type: bool,
        write_with_content_disposition: bool,
        write_with_cache_control: bool,
        write_multi_max_size: usize,
        write_multi_min_size: usize,
        write_total_max_size: usize,
        create_dir: bool,
        delete_feature: bool,
        copy: bool,
        rename: bool,
        list: bool,
        list_with_limit: bool,
        list_with_start_after: bool,
        list_with_recursive: bool,
        presign: bool,
        presign_read: bool,
        presign_stat: bool,
        presign_write: bool,
        shared: bool,
    }

    struct Metadata {
        mode: EntryMode,
        content_length: u64,
        cache_control: OptionalString,
        content_disposition: OptionalString,
        content_md5: OptionalString,
        content_type: OptionalString,
        content_encoding: OptionalString,
        etag: OptionalString,
        last_modified: OptionalString,
        version: OptionalString,
        is_current: OptionalBool,
        is_deleted: bool,
        // Note: content_range and user_metadata are complex types that need special handling
    }

    struct Entry {
        path: String,
    }

    extern "Rust" {
        type Operator;
        type Reader;
        type Lister;

        // cxx::UniquePtr isn't yet supported in stable Rust. We'll adopt it once support becomes available
        fn new_operator(scheme: &str, configs: Vec<HashMapValue>) -> Result<*mut Operator>;
        unsafe fn delete_operator(op: *mut Operator);

        fn read(self: &Operator, path: &str) -> Result<Vec<u8>>;
        fn write(self: &Operator, path: &str, bs: Vec<u8>) -> Result<()>;
        fn exists(self: &Operator, path: &str) -> Result<bool>;
        fn create_dir(self: &Operator, path: &str) -> Result<()>;
        fn copy(self: &Operator, src: &str, dst: &str) -> Result<()>;
        fn rename(self: &Operator, src: &str, dst: &str) -> Result<()>;
        fn remove(self: &Operator, path: &str) -> Result<()>;
        fn stat(self: &Operator, path: &str) -> Result<Metadata>;
        fn list(self: &Operator, path: &str) -> Result<Vec<Entry>>;
        fn reader(self: &Operator, path: &str) -> Result<*mut Reader>;
        fn lister(self: &Operator, path: &str) -> Result<*mut Lister>;
        fn info(self: &Operator) -> Result<Capability>;

        unsafe fn delete_reader(reader: *mut Reader);
        fn read(self: &mut Reader, buf: &mut [u8]) -> Result<usize>;
        fn seek(self: &mut Reader, offset: u64, dir: SeekFrom) -> Result<u64>;

        unsafe fn delete_lister(lister: *mut Lister);
        fn next(self: &mut Lister) -> Result<OptionalEntry>;
    }
}

pub struct Operator(od::blocking::Operator);

fn new_operator(scheme: &str, configs: Vec<ffi::HashMapValue>) -> Result<*mut Operator> {
    let map: HashMap<String, String> = configs
        .into_iter()
        .map(|value| (value.key, value.value))
        .collect();

    od::init_default_registry();

    let runtime =
        tokio::runtime::Handle::try_current().unwrap_or_else(|_| RUNTIME.handle().clone());
    let _guard = runtime.enter();

    let op = od::Operator::via_iter(scheme, map)?;
    let op = od::blocking::Operator::new(op)?;

    let op = Box::into_raw(Box::new(Operator(op)));

    Ok(op)
}

unsafe fn delete_operator(op: *mut Operator) {
    if !op.is_null() {
        unsafe {
            drop(Box::from_raw(op));
        }
    }
}

unsafe fn delete_reader(reader: *mut Reader) {
    if !reader.is_null() {
        unsafe {
            drop(Box::from_raw(reader));
        }
    }
}

unsafe fn delete_lister(lister: *mut Lister) {
    if !lister.is_null() {
        unsafe {
            drop(Box::from_raw(lister));
        }
    }
}

impl Operator {
    fn read(&self, path: &str) -> Result<Vec<u8>> {
        Ok(self.0.read(path)?.to_vec())
    }

    fn write(&self, path: &str, bs: Vec<u8>) -> Result<()> {
        Ok(self.0.write(path, bs).map(|_| ())?)
    }

    fn exists(&self, path: &str) -> Result<bool> {
        Ok(self.0.exists(path)?)
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

    fn reader(&self, path: &str) -> Result<*mut Reader> {
        let meta = self.0.stat(path)?;
        let reader = Box::into_raw(Box::new(Reader(
            self.0
                .reader(path)?
                .into_std_read(0..meta.content_length())?,
        )));
        Ok(reader)
    }

    fn lister(&self, path: &str) -> Result<*mut Lister> {
        let lister = Box::into_raw(Box::new(Lister(self.0.lister(path)?)));
        Ok(lister)
    }

    fn info(&self) -> Result<ffi::Capability> {
        let info = self.0.info();
        let cap = info.full_capability();

        Ok(ffi::Capability {
            stat: cap.stat,
            stat_with_if_match: cap.stat_with_if_match,
            stat_with_if_none_match: cap.stat_with_if_none_match,
            read: cap.read,
            read_with_if_match: cap.read_with_if_match,
            read_with_if_none_match: cap.read_with_if_none_match,
            read_with_override_cache_control: cap.read_with_override_cache_control,
            read_with_override_content_disposition: cap.read_with_override_content_disposition,
            read_with_override_content_type: cap.read_with_override_content_type,
            write: cap.write,
            write_can_multi: cap.write_can_multi,
            write_can_empty: cap.write_can_empty,
            write_can_append: cap.write_can_append,
            write_with_content_type: cap.write_with_content_type,
            write_with_content_disposition: cap.write_with_content_disposition,
            write_with_cache_control: cap.write_with_cache_control,
            write_multi_max_size: cap.write_multi_max_size.unwrap_or(0),
            write_multi_min_size: cap.write_multi_min_size.unwrap_or(0),
            write_total_max_size: cap.write_total_max_size.unwrap_or(0),
            create_dir: cap.create_dir,
            delete_feature: cap.delete,
            copy: cap.copy,
            rename: cap.rename,
            list: cap.list,
            list_with_limit: cap.list_with_limit,
            list_with_start_after: cap.list_with_start_after,
            list_with_recursive: cap.list_with_recursive,
            presign: cap.presign,
            presign_read: cap.presign_read,
            presign_stat: cap.presign_stat,
            presign_write: cap.presign_write,
            shared: cap.shared,
        })
    }
}
