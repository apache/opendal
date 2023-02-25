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

use std::env;
use std::io::Result;

use opendal::services;
use opendal::Operator;
use opendal::Scheme;

pub static OAY_ADDR: &str = "OAY_ADDR";
pub static OAY_BACKEND_TYPE: &str = "OAY_BACKEND_TYPE";

pub fn get_oay_addr() -> String {
    env::var(OAY_ADDR).unwrap_or_else(|_| "127.0.0.1:8080".to_string())
}

pub fn get_oay_backend_type() -> Result<Scheme> {
    Ok(env::var(OAY_BACKEND_TYPE)
        .unwrap_or_else(|_| "fs".to_string())
        .parse()?)
}

pub async fn get_oay_operator() -> Result<Operator> {
    let scheme = get_oay_backend_type()?;

    let prefix = format!("oay_backend_{scheme}_");
    let envs = env::vars().filter_map(move |(k, v)| {
        k.to_lowercase()
            .strip_prefix(&prefix)
            .map(|k| (k.to_string(), v))
    });

    let op = match scheme {
        Scheme::Azblob => Operator::from_iter::<services::Azblob>(envs)?.finish(),
        Scheme::Azdfs => Operator::from_iter::<services::Azdfs>(envs)?.finish(),
        Scheme::Fs => Operator::from_iter::<services::Fs>(envs)?.finish(),
        Scheme::Gcs => Operator::from_iter::<services::Gcs>(envs)?.finish(),
        Scheme::Ghac => Operator::from_iter::<services::Ghac>(envs)?.finish(),
        Scheme::Http => Operator::from_iter::<services::Http>(envs)?.finish(),
        Scheme::Ipmfs => Operator::from_iter::<services::Ipmfs>(envs)?.finish(),
        Scheme::Memory => Operator::from_iter::<services::Memory>(envs)?.finish(),
        Scheme::Obs => Operator::from_iter::<services::Obs>(envs)?.finish(),
        Scheme::Oss => Operator::from_iter::<services::Oss>(envs)?.finish(),
        Scheme::S3 => Operator::from_iter::<services::S3>(envs)?.finish(),
        Scheme::Webdav => Operator::from_iter::<services::Webdav>(envs)?.finish(),
        _ => unimplemented!("not supported services"),
    };

    Ok(op)
}
