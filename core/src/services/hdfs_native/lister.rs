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

use crate::raw::oio;
use crate::raw::oio::Entry;
use crate::*;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct HdfsNativeLister {
    path: String,
    client: Arc<hdfs_native::Client>,
}

impl HdfsNativeLister {
    pub fn new(path: String, client: Arc<hdfs_native::Client>) -> Self {
        HdfsNativeLister { path: path, client }
    }
}

impl oio::List for HdfsNativeLister {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Entry>>> {
        self.client.list_status(self.path.as_str(),false)
    }
}
