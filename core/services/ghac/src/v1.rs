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

#[derive(Clone, PartialEq, prost::Message)]
pub struct CreateCacheEntryRequest {
    #[prost(message, optional, tag = "1")]
    pub metadata: Option<CacheMetadata>,
    #[prost(string, tag = "2")]
    pub key: String,
    #[prost(string, tag = "3")]
    pub version: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CreateCacheEntryResponse {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    #[prost(string, tag = "2")]
    pub signed_upload_url: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct FinalizeCacheEntryUploadRequest {
    #[prost(message, optional, tag = "1")]
    pub metadata: Option<CacheMetadata>,
    #[prost(string, tag = "2")]
    pub key: String,
    #[prost(int64, tag = "3")]
    pub size_bytes: i64,
    #[prost(string, tag = "4")]
    pub version: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct GetCacheEntryDownloadUrlRequest {
    #[prost(message, optional, tag = "1")]
    pub metadata: Option<CacheMetadata>,
    #[prost(string, tag = "2")]
    pub key: String,
    #[prost(string, repeated, tag = "3")]
    pub restore_keys: Vec<String>,
    #[prost(string, tag = "4")]
    pub version: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct GetCacheEntryDownloadUrlResponse {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    #[prost(string, tag = "2")]
    pub signed_download_url: String,
    #[prost(string, tag = "3")]
    pub matched_key: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CacheMetadata {
    #[prost(int64, tag = "1")]
    pub repository_id: i64,
    #[prost(message, repeated, tag = "2")]
    pub scope: Vec<CacheScope>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CacheScope {
    #[prost(string, tag = "1")]
    pub scope: String,
    #[prost(int64, tag = "2")]
    pub permission: i64,
}
