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

use flutter_rust_bridge::frb;

use ::opendal as od;

use std::collections::HashMap;
use std::str::FromStr;

#[frb(opaque)]
pub struct Operator(opendal::Operator);

impl Operator {
    #[frb(sync)]
    pub fn new(scheme_str: String, map: HashMap<String, String>) -> Operator {
        let scheme: od::Scheme = od::Scheme::from_str(&scheme_str).unwrap();
        Self(od::Operator::via_iter(scheme, map).unwrap())
    }
    pub async fn stat(&self, path: String) -> Metadata {
        let meta = self.0.stat(&path).await.unwrap();

        Metadata(meta)
    }
    #[frb(sync)]
    pub fn stat_sync(&self, path: String) -> Metadata {
        let meta = self.0.blocking().stat(&path).unwrap();

        Metadata(meta)
    }
    pub async fn check(&self) -> () {
        self.0.check().await.unwrap()
    }
    pub async fn is_exist(&self, path: String) -> bool {
        self.0.is_exist(&path).await.unwrap()
    }
    pub async fn delete(&self, path: String) -> () {
        self.0.delete(&path).await.unwrap()
    }
    #[frb(sync)]
    pub fn delete_sync(&self, path: String) -> () {
        self.0.blocking().delete(&path).unwrap()
    }
    #[frb(sync)]
    pub fn is_exist_sync(&self, path: String) -> bool {
        self.0.blocking().is_exist(&path).unwrap()
    }
    pub async fn create_dir(&self, path: String) -> () {
        self.0.create_dir(&path).await.unwrap()
    }
    #[frb(sync)]
    pub fn create_dir_sync(&self, path: String) -> () {
        self.0.blocking().create_dir(&path).unwrap()
    }
    pub async fn rename(&self, from: String, to: String) -> () {
        self.0.rename(&from, &to).await.unwrap()
    }
    #[frb(sync)]
    pub fn rename_sync(&self, from: String, to: String) -> () {
        self.0
            .blocking()
            .rename(&from, &to)
            .unwrap()
    }
}

#[frb(opaque)]
pub struct Metadata(opendal::Metadata);

impl Metadata {
    /// Returns true if the <op.stat> object describes a file system directory.
    #[frb(sync, getter)]
    pub fn is_directory(&self) -> bool {
        self.0.is_dir()
    }

    /// Returns true if the <op.stat> object describes a regular file.
    #[frb(sync, getter)]
    pub fn is_file(&self) -> bool {
        self.0.is_file()
    }

    /// Content-Disposition of this object
    #[frb(sync, getter)]
    pub fn content_disposition(&self) -> Option<String> {
        self.0.content_disposition().map(|s| s.to_string())
    }

    /// Content Length of this object
    #[frb(sync, getter)]
    pub fn content_length(&self) -> Option<u64> {
        self.0.content_length().into()
    }

    /// Content MD5 of this object.
    #[frb(sync, getter)]
    pub fn content_md5(&self) -> Option<String> {
        self.0.content_md5().map(|s| s.to_string())
    }

    /// Content Type of this object.
    #[frb(sync, getter)]
    pub fn content_type(&self) -> Option<String> {
        self.0.content_type().map(|s| s.to_string())
    }

    /// ETag of this object.
    #[frb(sync, getter)]
    pub fn etag(&self) -> Option<String> {
        self.0.etag().map(|s| s.to_string())
    }

    /// Last Modified of this object.
    ///
    /// We will output this time in RFC3339 format like `1996-12-19T16:39:57+08:00`.
    #[frb(sync, getter)]
    pub fn last_modified(&self) -> Option<String> {
        self.0.last_modified().map(|ta| ta.to_rfc3339())
    }
}

