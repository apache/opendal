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

pub struct Writer(pub od::BlockingWriter);

impl Writer {
    // To avoid copying the bytes, we use &'static [u8] here.
    //
    // The safety should be guaranteed by the cpp caller.
    pub fn write(&mut self, buf: &'static [u8]) -> Result<()> {
        Ok(self.0.write(buf)?)
    }

    pub fn close(&mut self) -> Result<()> {
        Ok(self.0.close()?)
    }
}
