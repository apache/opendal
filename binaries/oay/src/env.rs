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

use std::env;
use std::io::Result;

use opendal::Operator;
use opendal::Scheme;

use crate::services;

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
        Scheme::Azblob => Operator::from_iter::<opendal::services::azblob::Builder>(envs)?.finish(),
        Scheme::Azdfs => todo!(),
        Scheme::Fs => todo!(),
        Scheme::Gcs => todo!(),
        Scheme::Ghac => todo!(),
        Scheme::Http => todo!(),
        Scheme::Ipmfs => todo!(),
        Scheme::Memory => todo!(),
        Scheme::Obs => todo!(),
        Scheme::Oss => todo!(),
        Scheme::S3 => todo!(),
        Scheme::Webdav => todo!(),
        Scheme::Custom(_) => todo!(),
        _ => todo!(),
    };

    Ok(op)
}
