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
use std::io::{Read, Seek};

use super::ffi;

pub struct Reader(pub od::StdReader);

impl Reader {
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = self.0.read(buf)?;
        Ok(n)
    }

    pub fn seek(&mut self, offset: u64, dir: ffi::SeekFrom) -> Result<u64> {
        let pos = match dir {
            ffi::SeekFrom::Start => std::io::SeekFrom::Start(offset),
            ffi::SeekFrom::Current => std::io::SeekFrom::Current(offset as i64),
            ffi::SeekFrom::End => std::io::SeekFrom::End(offset as i64),
            _ => return Err(anyhow::anyhow!("invalid seek dir")),
        };

        Ok(self.0.seek(pos)?)
    }
}
