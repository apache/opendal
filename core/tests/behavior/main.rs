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

// utils that used by tests
#[macro_use]
mod utils;

pub use utils::*;

// Async test cases
mod append;
mod copy;
mod list;
mod list_only;
mod presign;
mod read_only;
mod rename;
mod write;
use append::behavior_append_tests;
use copy::behavior_copy_tests;
use list::behavior_list_tests;
use list_only::behavior_list_only_tests;
use presign::behavior_presign_tests;
use read_only::behavior_read_only_tests;
use rename::behavior_rename_tests;
use write::behavior_write_tests;

// Blocking test cases
mod blocking_copy;
mod blocking_list;
mod blocking_read;
mod blocking_rename;
mod blocking_write;
use blocking_copy::behavior_blocking_copy_tests;
use blocking_list::behavior_blocking_list_tests;
use blocking_read::behavior_blocking_read_tests;
use blocking_rename::behavior_blocking_rename_tests;
use blocking_write::behavior_blocking_write_tests;

// External dependences
use libtest_mimic::{Arguments, Trial};
use opendal::*;
use tokio::runtime::Runtime;

fn behavior_test<B: Builder>() -> Vec<Trial> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let operator = match init_service::<B>() {
        Some(op) => op,
        None => return Vec::new(),
    };

    let mut trials = vec![];
    // Blocking tests
    trials.extend(behavior_blocking_copy_tests(&operator));
    trials.extend(behavior_blocking_list_tests(&operator));
    trials.extend(behavior_blocking_read_tests(&operator));
    trials.extend(behavior_blocking_rename_tests(&operator));
    trials.extend(behavior_blocking_write_tests(&operator));
    // Async tests
    trials.extend(behavior_append_tests(&runtime, &operator));
    trials.extend(behavior_copy_tests(&runtime, &operator));
    trials.extend(behavior_list_only_tests(&runtime, &operator));
    trials.extend(behavior_list_tests(&runtime, &operator));
    trials.extend(behavior_presign_tests(&runtime, &operator));
    trials.extend(behavior_read_only_tests(&runtime, &operator));
    trials.extend(behavior_rename_tests(&runtime, &operator));
    trials.extend(behavior_write_tests(&runtime, &operator));

    trials
}

fn main() -> anyhow::Result<()> {
    let args = Arguments::from_args();

    let mut tests = Vec::new();

    #[cfg(feature = "services-azblob")]
    tests.extend(behavior_test::<services::Azblob>());
    #[cfg(feature = "services-azdfs")]
    tests.extend(behavior_test::<services::Azdfs>());
    #[cfg(feature = "services-cos")]
    tests.extend(behavior_test::<services::Cos>());
    #[cfg(feature = "services-dashmap")]
    tests.extend(behavior_test::<services::Dashmap>());
    #[cfg(feature = "services-fs")]
    tests.extend(behavior_test::<services::Fs>());
    #[cfg(feature = "services-ftp")]
    tests.extend(behavior_test::<services::Ftp>());
    #[cfg(feature = "services-gcs")]
    tests.extend(behavior_test::<services::Gcs>());
    #[cfg(feature = "services-ghac")]
    tests.extend(behavior_test::<services::Ghac>());
    #[cfg(feature = "services-hdfs")]
    tests.extend(behavior_test::<services::Hdfs>());
    #[cfg(feature = "services-http")]
    tests.extend(behavior_test::<services::Http>());
    #[cfg(feature = "services-ipfs")]
    tests.extend(behavior_test::<services::Ipfs>());
    #[cfg(feature = "services-ipmfs")]
    tests.extend(behavior_test::<services::Ipmfs>());
    #[cfg(feature = "services-memcached")]
    tests.extend(behavior_test::<services::Memcached>());
    #[cfg(feature = "services-memory")]
    tests.extend(behavior_test::<services::Memory>());
    #[cfg(feature = "services-moka")]
    tests.extend(behavior_test::<services::Moka>());
    #[cfg(feature = "services-obs")]
    tests.extend(behavior_test::<services::Obs>());
    #[cfg(feature = "services-onedrive")]
    tests.extend(behavior_test::<services::Onedrive>());
    #[cfg(feature = "services-gdrive")]
    tests.extend(behavior_test::<services::Gdrive>());
    #[cfg(feature = "services-dropbox")]
    tests.extend(behavior_test::<services::Dropbox>());
    #[cfg(feature = "services-oss")]
    tests.extend(behavior_test::<services::Oss>());
    #[cfg(feature = "services-redis")]
    tests.extend(behavior_test::<services::Redis>());
    #[cfg(feature = "services-rocksdb")]
    tests.extend(behavior_test::<services::Rocksdb>());
    #[cfg(feature = "services-s3")]
    tests.extend(behavior_test::<services::S3>());
    #[cfg(feature = "services-sftp")]
    tests.extend(behavior_test::<services::Sftp>());
    #[cfg(feature = "services-sled")]
    tests.extend(behavior_test::<services::Sled>());
    #[cfg(feature = "services-supabase")]
    tests.extend(behavior_test::<services::Supabase>());
    #[cfg(feature = "services-vercel-artifacts")]
    tests.extend(behavior_test::<services::VercelArtifacts>());
    #[cfg(feature = "services-wasabi")]
    tests.extend(behavior_test::<services::Wasabi>());
    #[cfg(feature = "services-webdav")]
    tests.extend(behavior_test::<services::Webdav>());
    #[cfg(feature = "services-webhdfs")]
    tests.extend(behavior_test::<services::Webhdfs>());

    libtest_mimic::run(&args, tests).exit();
}
