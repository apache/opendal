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

#[derive(Debug, Clone)]
pub enum IpfsRpcV0 {
    List,
    Read,
    Write,
    Stat,
    Create,
    Delete,
}

impl IpfsRpcV0 {
    pub fn to_path(&self) -> String {
        match self {
            IpfsRpcV0::List => "/api/v0/files/ls".to_string(),
            IpfsRpcV0::Read => "/api/v0/files/read".to_string(),
            IpfsRpcV0::Write => "/api/v0/files/write".to_string(),
            IpfsRpcV0::Stat => "/api/v0/files/stat".to_string(),
            IpfsRpcV0::Create => "/api/v0/files/write".to_string(),
            IpfsRpcV0::Delete => "/api/v0/files/rm".to_string(),
        }
    }
}
