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

use clap::Parser;
use url::Url;

#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Config {
    /// fuse mount path
    #[arg(env = "OFS_MOUNT_PATH", index = 1)]
    pub mount_path: String,

    /// location of opendal service
    /// format: <scheme>://?<key>=<value>&<key>=<value>
    /// example: fs://?root=/tmp
    #[arg(env = "OFS_BACKEND", index = 2)]
    pub backend: Url,
}
