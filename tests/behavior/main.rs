// Copyright 2022 Datafuse Labs
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

#[macro_use]
mod base;
#[macro_use]
mod blocking_list;
#[macro_use]
mod blocking_read;
#[macro_use]
mod blocking_write;
#[macro_use]
mod list;
#[macro_use]
mod list_only;
#[macro_use]
mod multipart;
#[macro_use]
mod multipart_presign;
#[macro_use]
mod presign;
#[macro_use]
mod read_only;
#[macro_use]
mod write;

mod utils;

/// Generate real test cases.
/// Update function list while changed.
macro_rules! behavior_tests {
    ($($service:ident),*) => {
        $(
            behavior_base_tests!($service);
            // can_read && !can_write
            behavior_read_tests!($service);
            // can_read && !can_write && can_blocking
            behavior_blocking_read_tests!($service);
            // can_read && can_write
            behavior_write_tests!($service);
            // can_read && can_write && can_blocking
            behavior_blocking_write_tests!($service);
            // can_read && can_write && can_list
            behavior_list_tests!($service);
            // can_read && can_write && can_presign
            behavior_presign_tests!($service);
            // can_read && can_write && can_blocking && can_list
            behavior_blocking_list_tests!($service);
            // can_read && can_write && can_multipart
            behavior_multipart_tests!($service);
            // can_read && can_write && can_multipart && can_presign
            behavior_multipart_presign_tests!($service);
            // can_list && !can_write
            behavior_list_only_tests!($service);
        )*
    };
}

behavior_tests!(Azblob);
behavior_tests!(Azdfs);
cfg_if::cfg_if! { if #[cfg(feature = "services-dashmap")] { behavior_tests!(Dashmap); }}
behavior_tests!(Fs);
cfg_if::cfg_if! { if #[cfg(feature = "services-ftp")] { behavior_tests!(Ftp); }}
cfg_if::cfg_if! { if #[cfg(feature = "services-memcached")] { behavior_tests!(Memcached); }}
behavior_tests!(Memory);
cfg_if::cfg_if! { if #[cfg(feature = "services-moka")] { behavior_tests!(Moka); }}
behavior_tests!(Gcs);
behavior_tests!(Ghac);
cfg_if::cfg_if! { if #[cfg(feature = "services-ipfs")] { behavior_tests!(Ipfs); }}
behavior_tests!(Ipmfs);
cfg_if::cfg_if! { if #[cfg(feature = "services-hdfs")] { behavior_tests!(Hdfs); }}
cfg_if::cfg_if! { if #[cfg(feature = "services-http")] { behavior_tests!(Http); }}
behavior_tests!(Obs);
cfg_if::cfg_if! { if #[cfg(feature = "services-redis")] { behavior_tests!(Redis); }}
cfg_if::cfg_if! { if #[cfg(feature = "services-rocksdb")] { behavior_tests!(Rocksdb); }}
behavior_tests!(Oss);
behavior_tests!(S3);
cfg_if::cfg_if! { if #[cfg(feature = "services-sled")] { behavior_tests!(Sled); }}
behavior_tests!(Webdav);
behavior_tests!(Webhdfs);
