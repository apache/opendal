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

use crate::raw::*;
use crate::*;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashMap;

/// Builder is used to set up a real underlying service, i.e. storage accessor.
///
/// One builder is usually used by [`Operator`] during its initialization.
/// It can be created by accepting several k-v pairs from one HashMap, one iterator and specific environment variables.
///
/// By default each builder of underlying service must support deriving from one HashMap.
/// Besides that, according to the implementation, each builder will have its own special methods
/// to control the behavior of initialization of the underlying service.
/// It often provides semantic interface instead of using dynamic k-v strings directly.
/// Because the latter way is obscure and hard to remember how many parameters it will have.
///
/// So it is recommended that developer should read related doc of builder carefully when you are working with one service.
/// We also promise that every public API will provide detailed documentation.
///
/// It's recommended to use [`Operator::new`] to avoid use `Builder` trait directly.
pub trait Builder: Default {
    /// Associated scheme for this builder. It indicates what underlying service is.
    const SCHEME: Scheme;
    /// The accessor that built by this builder.
    type Accessor: Access;
    /// The config for this builder.
    type Config: DeserializeOwned;

    /// Construct a builder from given config.
    fn from_config(config: Self::Config) -> Self;

    /// Construct a builder from given map which contains several parameters needed by underlying service.
    fn from_map(map: HashMap<String, String>) -> Result<Self> {
        match Self::Config::deserialize(ConfigDeserializer::new(map)) {
            Ok(config) => Ok(Self::from_config(config)),
            Err(err) => Err(
                Error::new(ErrorKind::ConfigInvalid, "failed to deserialize config")
                    .set_source(err),
            ),
        }
    }

    /// Consume the accessor builder to build a service.
    fn build(&mut self) -> Result<Self::Accessor>;
}

/// Dummy implementation of builder
impl Builder for () {
    const SCHEME: Scheme = Scheme::Custom("dummy");

    type Accessor = ();

    type Config = ();

    fn from_config(_: Self::Config) -> Self {}

    fn build(&mut self) -> Result<Self::Accessor> {
        Ok(())
    }
}
