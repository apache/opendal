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

#![deny(clippy::all)]

use std::collections::HashMap;
use std::str;

use opendal::Builder;
use crate::{DataObject, Operator};

#[napi]
pub struct Memory(opendal::Operator);

#[napi]
impl Memory {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self(opendal::Operator::create(opendal::services::Memory::default())
            .unwrap()
            .finish()
        )
    }

    #[napi]
    pub fn object(&self, path: String) -> DataObject {
        DataObject(self.0.object(&path))
    }
}

#[napi]
pub struct Fs(opendal::Operator);

#[napi]
impl Fs {
    #[napi(constructor)]
    pub fn new(options: HashMap<String, String>) -> Operator {
        Operator(opendal::Operator::create(opendal::services::Fs::from_map(options))
            .unwrap()
            .finish()
        )
    }

    #[napi]
    pub fn object(&self, path: String) -> DataObject {
        DataObject(self.0.object(&path))
    }
}
