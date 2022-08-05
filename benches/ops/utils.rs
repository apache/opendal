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

use bytes::Bytes;
use once_cell::sync::Lazy;
use opendal::Operator;
use opendal::Scheme;
use rand::prelude::*;

pub static TOKIO: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().expect("build tokio runtime"));

async fn service(scheme: Scheme) -> Option<Operator> {
    let test_key = format!("opendal_{scheme}_test");
    if let Ok(test) = env::var(test_key) {
        if test == "on" {
            Some(
                Operator::from_env(scheme).unwrap_or_else(|_| panic!("init {scheme} must succeed")),
            )
        } else {
            None
        }
    } else {
        None
    }
}

pub fn services() -> Vec<(&'static str, Option<Operator>)> {
    TOKIO.block_on(async {
        vec![
            ("fs", service(Scheme::Fs).await),
            ("s3", service(Scheme::S3).await),
            ("memory", service(Scheme::Memory).await),
        ]
    })
}

pub fn gen_bytes(rng: &mut ThreadRng, size: usize) -> Vec<u8> {
    let mut content = vec![0; size as usize];
    rng.fill_bytes(&mut content);

    content
}

pub struct TempData {
    op: Operator,
    path: String,
}

impl TempData {
    pub fn existing(op: Operator, path: &str) -> Self {
        Self {
            op,
            path: path.to_string(),
        }
    }

    pub fn generate(op: Operator, path: &str, content: Vec<u8>) -> Self {
        TOKIO.block_on(async {
            op.object(path)
                .write(Bytes::from(content))
                .await
                .expect("create test data")
        });

        Self {
            op,
            path: path.to_string(),
        }
    }
}

impl Drop for TempData {
    fn drop(&mut self) {
        TOKIO.block_on(async {
            self.op
                .object(&self.path)
                .delete()
                .await
                .expect("cleanup test data");
        })
    }
}
