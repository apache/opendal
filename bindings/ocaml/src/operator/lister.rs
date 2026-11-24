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

use super::*;

#[ocaml::func]
#[ocaml::sig("lister -> (entry option, string) Result.t ")]
pub fn lister_next(lister: &Lister) -> Result<Option<ocaml::Pointer<Entry>>, String> {
    let next = {
        let mut lister = lister
            .0
            .lock()
            .map_err(|_| "lister mutex poisoned".to_string())?;
        lister.next()
    };
    match next {
        Some(Ok(entry)) => Ok(Some(Entry(entry).into())),
        Some(Err(e)) => Err(e.to_string()),
        None => Ok(None),
    }
}
