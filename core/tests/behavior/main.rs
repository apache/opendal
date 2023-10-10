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
mod fuzz;
mod list;
mod list_only;
mod presign;
mod read_only;
mod rename;
mod write;
use append::behavior_append_tests;
use copy::behavior_copy_tests;
use fuzz::behavior_fuzz_tests;
use list::behavior_list_tests;
use list_only::behavior_list_only_tests;
use presign::behavior_presign_tests;
use read_only::behavior_read_only_tests;
use rename::behavior_rename_tests;
use write::behavior_write_tests;

// Blocking test cases
mod blocking_append;
mod blocking_copy;
mod blocking_list;
mod blocking_read_only;
mod blocking_rename;
mod blocking_write;

use blocking_append::behavior_blocking_append_tests;
use blocking_copy::behavior_blocking_copy_tests;
use blocking_list::behavior_blocking_list_tests;
use blocking_read_only::behavior_blocking_read_only_tests;
use blocking_rename::behavior_blocking_rename_tests;
use blocking_write::behavior_blocking_write_tests;
// External dependences
use libtest_mimic::{Arguments, Trial};
use once_cell::sync::Lazy;
use opendal::*;

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

fn behavior_test<B: Builder>() -> Vec<Trial> {
    let operator = match init_service::<B>() {
        Some(op) => op,
        None => return Vec::new(),
    };

    let mut trials = vec![];
    // Blocking tests
    trials.extend(behavior_blocking_append_tests(&operator));
    trials.extend(behavior_blocking_copy_tests(&operator));
    trials.extend(behavior_blocking_list_tests(&operator));
    trials.extend(behavior_blocking_read_only_tests(&operator));
    trials.extend(behavior_blocking_rename_tests(&operator));
    trials.extend(behavior_blocking_write_tests(&operator));
    // Async tests
    trials.extend(behavior_append_tests(&operator));
    trials.extend(behavior_copy_tests(&operator));
    trials.extend(behavior_list_only_tests(&operator));
    trials.extend(behavior_list_tests(&operator));
    trials.extend(behavior_presign_tests(&operator));
    trials.extend(behavior_read_only_tests(&operator));
    trials.extend(behavior_rename_tests(&operator));
    trials.extend(behavior_write_tests(&operator));
    trials.extend(behavior_fuzz_tests(&operator));

    trials
}

fn main() -> anyhow::Result<()> {
    let args = Arguments::from_args();

    let mut tests = Vec::new();

    #[cfg(feature = "services-atomicserver")]
    tests.extend(behavior_test::<services::Atomicserver>());
    #[cfg(feature = "services-azblob")]
    tests.extend(behavior_test::<services::Azblob>());
    #[cfg(feature = "services-azdls")]
    tests.extend(behavior_test::<services::Azdls>());
    #[cfg(feature = "services-cacache")]
    tests.extend(behavior_test::<services::Cacache>());
    #[cfg(feature = "services-cos")]
    tests.extend(behavior_test::<services::Cos>());
    #[cfg(feature = "services-dashmap")]
    tests.extend(behavior_test::<services::Dashmap>());
    #[cfg(feature = "services-etcd")]
    tests.extend(behavior_test::<services::Etcd>());
    #[cfg(feature = "services-foundationdb")]
    tests.extend(behavior_test::<services::Foundationdb>());
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
    #[cfg(feature = "services-libsql")]
    tests.extend(behavior_test::<services::Libsql>());
    #[cfg(feature = "services-memcached")]
    tests.extend(behavior_test::<services::Memcached>());
    #[cfg(feature = "services-memory")]
    tests.extend(behavior_test::<services::Memory>());
    #[cfg(feature = "services-mini-moka")]
    tests.extend(behavior_test::<services::MiniMoka>());
    #[cfg(feature = "services-moka")]
    tests.extend(behavior_test::<services::Moka>());
    #[cfg(feature = "services-obs")]
    tests.extend(behavior_test::<services::Obs>());
    #[cfg(feature = "services-onedrive")]
    tests.extend(behavior_test::<services::Onedrive>());
    #[cfg(feature = "services-postgresql")]
    tests.extend(behavior_test::<services::Postgresql>());
    #[cfg(feature = "services-gdrive")]
    tests.extend(behavior_test::<services::Gdrive>());
    #[cfg(feature = "services-dropbox")]
    tests.extend(behavior_test::<services::Dropbox>());
    #[cfg(feature = "services-oss")]
    tests.extend(behavior_test::<services::Oss>());
    #[cfg(feature = "services-persy")]
    tests.extend(behavior_test::<services::Persy>());
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
    #[cfg(feature = "services-redb")]
    tests.extend(behavior_test::<services::Redb>());
    #[cfg(feature = "services-tikv")]
    tests.extend(behavior_test::<services::Tikv>());
    #[cfg(feature = "services-mysql")]
    tests.extend(behavior_test::<services::Mysql>());
    #[cfg(feature = "services-sqlite")]
    tests.extend(behavior_test::<services::Sqlite>());

    // Don't init logging while building operator which may break cargo
    // nextest output
    let _ = tracing_subscriber::fmt()
        .pretty()
        .with_test_writer()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    libtest_mimic::run(&args, tests).exit();
}
