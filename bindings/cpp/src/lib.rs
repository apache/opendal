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
use std::io::{BufRead, BufReader, Seek};
use std::str::FromStr;

#[cxx::bridge(namespace = "opendal::ffi")]
mod ffi {
    struct HashMapValue {
        key: String,
        value: String,
    }

    enum SeekDir {
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

    struct Metadata {
        mode: EntryMode,
        content_length: u64,
        cache_control: OptionalString,
        content_disposition: OptionalString,
        content_md5: OptionalString,
        content_type: OptionalString,
        etag: OptionalString,
        last_modified: OptionalString,
    }

    struct Entry {
        path: String,
    }

    extern "Rust" {
        type Operator;
        type Reader;

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
        fn reader(self: &Operator, path: &str) -> Result<Box<Reader>>;

        fn fill_buf(self: &mut Reader) -> Result<&[u8]>;
        fn buffer(self: &Reader) -> &[u8];
        fn consume(self: &mut Reader, amt: usize);
        fn seek(self: &mut Reader, offset: u64, dir: SeekDir) -> Result<u64>;
    }
}

struct Operator(od::BlockingOperator);
struct Reader(BufReader<od::BlockingReader>);

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

    fn reader(&self, path: &str) -> Result<Box<Reader>> {
        Ok(Box::new(Reader(BufReader::new(self.0.reader(path)?))))
    }
}

impl Reader {
    fn fill_buf(&mut self) -> Result<&[u8]> {
        Ok(self.0.fill_buf()?)
    }

    fn buffer(&self) -> &[u8] {
        self.0.buffer()
    }

    fn consume(&mut self, amt: usize) {
        self.0.consume(amt)
    }

    fn seek(&mut self, offset: u64, dir: ffi::SeekDir) -> Result<u64> {
        let pos = match dir {
            ffi::SeekDir::Start => std::io::SeekFrom::Start(offset),
            ffi::SeekDir::Current => std::io::SeekFrom::Current(offset as i64),
            ffi::SeekDir::End => std::io::SeekFrom::End(offset as i64),
            _ => return Err(anyhow::anyhow!("invalid seek dir")),
        };

        Ok(self.0.seek(pos)?)
    }
}

impl From<od::Metadata> for ffi::Metadata {
    fn from(meta: od::Metadata) -> Self {
        let mode = meta.mode().into();
        let content_length = meta.content_length();
        let cache_control = meta.cache_control().map(ToOwned::to_owned).into();
        let content_disposition = meta.content_disposition().map(ToOwned::to_owned).into();
        let content_md5 = meta.content_md5().map(ToOwned::to_owned).into();
        let content_type = meta.content_type().map(ToOwned::to_owned).into();
        let etag = meta.etag().map(ToOwned::to_owned).into();
        let last_modified = meta
            .last_modified()
            .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Nanos, false))
            .into();

        Self {
            mode,
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

impl From<od::EntryMode> for ffi::EntryMode {
    fn from(mode: od::EntryMode) -> Self {
        match mode {
            od::EntryMode::FILE => Self::File,
            od::EntryMode::DIR => Self::Dir,
            _ => Self::Unknown,
        }
    }
}

impl From<Option<String>> for ffi::OptionalString {
    fn from(s: Option<String>) -> Self {
        match s {
            Some(s) => Self {
                has_value: true,
                value: s,
            },
            None => Self {
                has_value: false,
                value: String::default(),
            },
        }
    }
}
