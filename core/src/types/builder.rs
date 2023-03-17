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

use std::collections::HashMap;
use std::env;

use crate::raw::*;
use crate::*;

/// Builder is used to build a storage accessor used by [`Operator`].
///
/// It's recommended to use [`Operator::new`] to avoid use `Builder` trait directly.
pub trait Builder: Default {
    /// Associated scheme for this builder.
    const SCHEME: Scheme;
    /// The accessor that built by this builder.
    type Accessor: Accessor;

    /// Construct a builder from given map.
    fn from_map(map: HashMap<String, String>) -> Self;

    /// Construct a builder from given iterator.
    fn from_iter(iter: impl Iterator<Item = (String, String)>) -> Self
    where
        Self: Sized,
    {
        Self::from_map(iter.collect())
    }

    /// Construct a builder from envs.
    fn from_env() -> Self
    where
        Self: Sized,
    {
        let prefix = format!("opendal_{}_", Self::SCHEME);
        let envs = env::vars()
            .filter_map(move |(k, v)| {
                k.to_lowercase()
                    .strip_prefix(&prefix)
                    .map(|k| (k.to_string(), v))
            })
            .collect();

        Self::from_map(envs)
    }

    /// Consume the accessor builder to build a service.
    fn build(&mut self) -> Result<Self::Accessor>;
}

/// Dummy implementation of builder
impl Builder for () {
    const SCHEME: Scheme = Scheme::Custom("dummy");

    type Accessor = ();

    fn from_map(_: HashMap<String, String>) -> Self {}

    fn build(&mut self) -> Result<Self::Accessor> {
        Ok(())
    }
}
