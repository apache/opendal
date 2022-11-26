// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Providing specific services support.
//!
//! In order to implement a service, we need the following things:
//!
//! - Builder: responsible for building the service backend.
//! - Backend: the service backend which implements the [`Accessor`][crate::raw::Accessor] trait.

pub mod azblob;
pub mod azdfs;
pub mod fs;
#[cfg(feature = "services-ftp")]
pub mod ftp;
pub mod gcs;
#[cfg(feature = "services-hdfs")]
pub mod hdfs;
pub mod http;
#[cfg(feature = "services-ipfs")]
pub mod ipfs;
pub mod ipmfs;
pub mod memory;
#[cfg(feature = "services-moka")]
pub mod moka;
pub mod obs;
pub mod oss;
#[cfg(feature = "services-redis")]
pub mod redis;
#[cfg(feature = "services-rocksdb")]
pub mod rocksdb;
pub mod s3;
