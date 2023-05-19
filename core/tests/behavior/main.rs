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

#[macro_use]
mod blocking_copy;
#[macro_use]
mod blocking_list;
#[macro_use]
mod blocking_rename;
#[macro_use]
mod blocking_read;
#[macro_use]
mod blocking_write;
#[macro_use]
mod copy;
#[macro_use]
mod list;
#[macro_use]
mod list_only;
#[macro_use]
mod presign;
#[macro_use]
mod read_only;
#[macro_use]
mod rename;
#[macro_use]
mod write;

mod utils;

/// Generate real test cases.
/// Update function list while changed.
macro_rules! behavior_tests {
    ($($service:ident),*) => {
        paste::item! {
            $(
                mod [<services_ $service:snake>] {
                    use once_cell::sync::Lazy;

                    static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
                       tokio::runtime::Builder::new_multi_thread()
                            .enable_all()
                            .build()
                            .unwrap()
                    });
                    static OPERATOR: Lazy<Option<opendal::Operator>> = Lazy::new(||
                        $crate::utils::init_service::<opendal::services::$service>()
                    );

                    // can_read && !can_write
                    behavior_read_tests!($service);
                    // can_read && !can_write && can_blocking
                    behavior_blocking_read_tests!($service);
                    // can_read && can_write
                    behavior_write_tests!($service);
                    // can_read && can_write && can_blocking
                    behavior_blocking_write_tests!($service);
                    // can_read && can_write && can_copy
                    behavior_copy_tests!($service);
                    // can read && can_write && can_blocking && can_copy
                    behavior_blocking_copy_tests!($service);
                    // can_read && can_write && can_move
                    behavior_rename_tests!($service);
                    // can_read && can_write && can_blocking && can_move
                    behavior_blocking_rename_tests!($service);
                    // can_read && can_write && can_list
                    behavior_list_tests!($service);
                    // can_read && can_write && can_presign
                    behavior_presign_tests!($service);
                    // can_read && can_write && can_blocking && can_list
                    behavior_blocking_list_tests!($service);
                    // can_list && !can_write
                    behavior_list_only_tests!($service);
                }
         )*
        }
    };
}

#[cfg(feature = "services-azblob")]
behavior_tests!(Azblob);
#[cfg(feature = "services-azdfs")]
behavior_tests!(Azdfs);
#[cfg(feature = "services-cos")]
behavior_tests!(Cos);
#[cfg(feature = "services-dashmap")]
behavior_tests!(Dashmap);
#[cfg(feature = "services-fs")]
behavior_tests!(Fs);
#[cfg(feature = "services-ftp")]
behavior_tests!(Ftp);
#[cfg(feature = "services-memcached")]
behavior_tests!(Memcached);
#[cfg(feature = "services-memory")]
behavior_tests!(Memory);
#[cfg(feature = "services-moka")]
behavior_tests!(Moka);
#[cfg(feature = "services-gcs")]
behavior_tests!(Gcs);
#[cfg(feature = "services-ghac")]
behavior_tests!(Ghac);
#[cfg(feature = "services-ipfs")]
behavior_tests!(Ipfs);
#[cfg(feature = "services-ipmfs")]
behavior_tests!(Ipmfs);
#[cfg(feature = "services-hdfs")]
behavior_tests!(Hdfs);
#[cfg(feature = "services-http")]
behavior_tests!(Http);
#[cfg(feature = "services-obs")]
behavior_tests!(Obs);
#[cfg(feature = "services-redis")]
behavior_tests!(Redis);
#[cfg(feature = "services-rocksdb")]
behavior_tests!(Rocksdb);
#[cfg(feature = "services-oss")]
behavior_tests!(Oss);
#[cfg(feature = "services-s3")]
behavior_tests!(S3);
#[cfg(feature = "services-sftp")]
behavior_tests!(Sftp);
#[cfg(feature = "services-supabase")]
behavior_tests!(Supabase);
#[cfg(feature = "services-sled")]
behavior_tests!(Sled);
#[cfg(feature = "services-vercel-artifacts")]
behavior_tests!(VercelArtifacts);
#[cfg(feature = "services-wasabi")]
behavior_tests!(Wasabi);
#[cfg(feature = "services-webdav")]
behavior_tests!(Webdav);
#[cfg(feature = "services-webhdfs")]
behavior_tests!(Webhdfs);
#[cfg(feature = "services-onedrive")]
behavior_tests!(Onedrive);
