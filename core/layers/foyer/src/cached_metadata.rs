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

/// Cached metadata for an object in chunked mode.
///
/// This stores essential information needed for chunk-based reading:
/// - `content_length`: Total size of the object
/// - `version`: Object version (if versioning is enabled)
/// - `etag`: Entity tag for cache validation
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CachedMetadata {
    pub content_length: u64,
    pub version: Option<String>,
    pub etag: Option<String>,
}
