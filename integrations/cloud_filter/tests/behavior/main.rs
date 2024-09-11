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

mod fetch_data;
mod fetch_placeholder;
mod utils;

use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    process::ExitCode,
};

use cloud_filter::{
    filter::AsyncBridge,
    root::{
        Connection, HydrationType, PopulationType, SecurityId, Session, SyncRootId,
        SyncRootIdBuilder, SyncRootInfo,
    },
};
use cloudfilter_opendal::CloudFilter;
use libtest_mimic::{Arguments, Trial};
use opendal::{raw::tests, Operator};
use tokio::runtime::Handle;

const PROVIDER_NAME: &str = "ro-cloudfilter";
const DISPLAY_NAME: &str = "Test Cloud Filter";
const ROOT_PATH: &str = "C:\\sync_root";

#[tokio::main]
async fn main() -> ExitCode {
    let args = Arguments::from_args();

    env_logger::init();

    let Ok(Some(op)) = tests::init_test_service() else {
        return ExitCode::SUCCESS;
    };

    if !Path::new(ROOT_PATH).try_exists().expect("try exists") {
        fs::create_dir(ROOT_PATH).expect("create root dir");
    }

    let (sync_root_id, connection) = init(op);

    let tests = vec![
        Trial::test("fetch_data", fetch_data::test_fetch_data),
        Trial::test(
            "fetch_placeholder",
            fetch_placeholder::test_fetch_placeholder,
        ),
    ];

    let conclusion = libtest_mimic::run(&args, tests);

    drop(connection);
    sync_root_id.unregister().unwrap();
    fs::remove_dir_all(ROOT_PATH).expect("remove root dir");

    conclusion.exit_code()
}

fn init(
    op: Operator,
) -> (
    SyncRootId,
    Connection<AsyncBridge<CloudFilter, impl Fn(Pin<Box<dyn Future<Output = ()>>>)>>,
) {
    let sync_root_id = SyncRootIdBuilder::new(PROVIDER_NAME)
        .user_security_id(SecurityId::current_user().unwrap())
        .build();

    if !sync_root_id.is_registered().unwrap() {
        sync_root_id
            .register(
                SyncRootInfo::default()
                    .with_display_name(DISPLAY_NAME)
                    .with_hydration_type(HydrationType::Full)
                    .with_population_type(PopulationType::Full)
                    .with_icon("%SystemRoot%\\system32\\charmap.exe,0")
                    .with_version("1.0.0")
                    .with_recycle_bin_uri("http://cloudmirror.example.com/recyclebin")
                    .unwrap()
                    .with_path(ROOT_PATH)
                    .unwrap(),
            )
            .unwrap();
    }

    let handle = Handle::current();
    let connection = Session::new()
        .connect_async(
            ROOT_PATH,
            CloudFilter::new(op, PathBuf::from(&ROOT_PATH)),
            move |f| handle.clone().block_on(f),
        )
        .expect("create session");

    (sync_root_id, connection)
}
