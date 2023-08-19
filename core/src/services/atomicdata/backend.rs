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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;

use atomic_lib::errors::AtomicError;
use atomic_lib::storelike::Query;
use atomic_lib::Store;
use atomic_lib::Storelike;

use async_trait::async_trait;

use crate::raw::adapters::kv;
use crate::raw::normalize_root;
use crate::Builder;
use crate::Scheme;
use crate::*;

/// Atomicdata service support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct AtomicdataBuilder {
    root: Option<String>,
    endpoint: Option<String>,
}

impl AtomicdataBuilder {
    /// Set the root for Atomicdata.
    pub fn root(&mut self, path: &str) -> &mut Self {
        self.root = Some(path.into());
        self
    }

    /// Set the server address for Atomicdata.
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = Some(endpoint.into());
        self
    }
}

impl Builder for AtomicdataBuilder {
    const SCHEME: Scheme = Scheme::Atomicdata;
    type Accessor = AtomicdataBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = AtomicdataBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("endpoint").map(|v| builder.endpoint(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let store = atomic_lib::Store::init().unwrap();

        let agent = store.create_agent(Some("local_agent")).unwrap();
        store.set_default_agent(agent);

        let store = Arc::new(store);

        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let endpoint = self.endpoint.clone().unwrap();

        Ok(AtomicdataBackend::new(Adapter { store, endpoint }).with_root(&root))
    }
}

/// Backend for Atomicdata services.
pub type AtomicdataBackend = kv::Backend<Adapter>;

const DOCUMENT_CLASS: &str = "classes/Document";
const KEY_PROPERTY: &str = "properties/name";
const VALUE_PROPERTY: &str = "properties/atom/value";

#[derive(Clone)]
pub struct Adapter {
    store: Arc<Store>,
    endpoint: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.finish()
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Atomicdata,
            "atomicdata",
            Capability {
                read: true,
                write: true,
                delete: true,
                create_dir: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let query =
            Query::new_prop_val(format!("{}/{}", self.endpoint, KEY_PROPERTY).as_str(), path);
        let query_result = self.store.query(&query).map_err(format_atomic_error)?;

        if query_result.resources.is_empty() {
            return Err(Error::new(ErrorKind::NotFound, "atomicdata: key not found"));
        }

        let result = query_result.resources[0]
            .get(format!("{}/{}", self.endpoint, VALUE_PROPERTY).as_str())
            .map_err(format_atomic_error)?;

        let data = BASE64_STANDARD.decode(result.to_string()).unwrap();

        Ok(Some(data))
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        let mut subject;

        let query =
            Query::new_prop_val(format!("{}/{}", self.endpoint, KEY_PROPERTY).as_str(), path);
        let query_result = self.store.query(&query).map_err(format_atomic_error)?;

        if !query_result.resources.is_empty() {
            subject = query_result.resources[0].clone();
        } else {
            subject = atomic_lib::Resource::new_instance(
                format!("{}/{}", self.endpoint, DOCUMENT_CLASS).as_str(),
                &*self.store,
            )
            .map_err(format_atomic_error)?;

            subject
                .set_propval_string(
                    format!("{}/{}", self.endpoint, KEY_PROPERTY).to_string(),
                    path,
                    &*self.store,
                )
                .map_err(format_atomic_error)?;
        }

        subject
            .set_propval_string(
                format!("{}/{}", self.endpoint, VALUE_PROPERTY).to_string(),
                &BASE64_STANDARD.encode(value),
                &*self.store,
            )
            .map_err(format_atomic_error)?;

        subject
            .save_locally(&*self.store)
            .map_err(format_atomic_error)?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let query =
            Query::new_prop_val(format!("{}/{}", self.endpoint, KEY_PROPERTY).as_str(), path);
        let query_result = self.store.query(&query).map_err(format_atomic_error)?;

        if !query_result.resources.is_empty() {
            self.store
                .remove_resource(query_result.resources[0].get_subject())
                .map_err(format_atomic_error)?;
        }

        Ok(())
    }
}

fn format_atomic_error(err: AtomicError) -> Error {
    Error::new(ErrorKind::Unexpected, &err.message)
        .with_context("service", Scheme::Atomicdata)
        .set_source(err)
}
