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

// Behavior test suites.
mod behavior;
#[macro_use]
mod read;
#[macro_use]
mod blocking_read;
mod utils;

pub fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

/// Generate real test cases.
/// Update function list while changed.
macro_rules! behavior_tests {
    ($($service:ident),*) => {
        $(
            behavior_read_tests!($service);
            behavior_blocking_read_tests!($service);
        )*
    };
}

behavior_tests!(Azblob);
behavior_tests!(Fs);
cfg_if::cfg_if! { if #[cfg(feature = "services-ftp")] { behavior_tests!(Ftp); }}
behavior_tests!(Memory);
behavior_tests!(Gcs);
behavior_tests!(Ipmfs);
cfg_if::cfg_if! { if #[cfg(feature = "services-hdfs")] { behavior_tests!(Hdfs); }}
cfg_if::cfg_if! { if #[cfg(feature = "services-http")] {behavior_tests!(Http); }}
behavior_tests!(Obs);
behavior_tests!(S3);
