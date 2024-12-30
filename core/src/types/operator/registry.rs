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

use std::cell::LazyCell;
use std::collections::HashMap;

use crate::services::*;
use crate::*;

// TODO: thread local or use LazyLock instead? this way the access is lock-free
// TODO: should we expose the `GLOBAL_OPERATOR_REGISTRY` as public API at `crate::types::operator::GLOBAL_OPERATOR_REGISTRY`?
thread_local! {
    pub static GLOBAL_OPERATOR_REGISTRY: LazyCell<OperatorRegistry> = LazyCell::new(|| OperatorRegistry::with_enabled_services());
}

// In order to reduce boilerplate, we should return in this function a `Builder` instead of operator?.
pub type OperatorFactory = fn(&str, HashMap<String, String>) -> Result<Operator>;

// TODO: create an static registry? or a global() method of OperatorRegistry that lazily initializes the registry?
// Register only services in `Scheme::enabled()`

pub struct OperatorRegistry {
    // TODO: add Arc<Mutex<...>> to make it cheap to clone + thread safe? or is it not needed?
    registry: HashMap<String, OperatorFactory>,
}

impl OperatorRegistry {
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
        }
    }

    pub fn register(&mut self, scheme: &str, factory: OperatorFactory) {
        // TODO: should we receive a `&str` or a `String`? we are cloning it anyway
        self.registry.insert(scheme.to_string(), factory);
    }

    pub fn parse(
        &self,
        uri: &str,
        options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Operator> {
        // TODO: we use the `url::Url` struct instead of `http:Uri`, because
        // we needed it in `Configurator::from_uri` method.
        let parsed_url = url::Url::parse(uri).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "uri is invalid")
                .with_context("uri", uri)
                .set_source(err)
        })?;

        // TODO: with the `url::Url` struct, we always have the scheme (it is not an Option<str>)
        // but with the `http::Uri` crate, it can be missing https://docs.rs/http/latest/http/uri/struct.Uri.html#method.scheme
        // which one should we use?
        let scheme = parsed_url.scheme();

        let factory = self.registry.get(scheme).ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "could not find any operator factory for the given scheme",
            )
            .with_context("uri", uri)
            .with_context("scheme", scheme)
        })?;

        // TODO: `OperatorFactory` should receive `IntoIterator<Item = (String, String)>` instead of `HashMap<String, String>`?
        // however, impl Traits in type aliases is unstable and also are not allowed in fn pointers
        let options = options.into_iter().collect();

        // TODO: `OperatorFactory` should use `&str` instead of `String`? we are cloning it anyway
        factory(uri, options)
    }

    pub fn with_enabled_services() -> Self {
        let mut registry = Self::new();
        // TODO: is this correct? have a `Builder::enabled()` method that returns the set of enabled services builders?
        // Similar to `Scheme::Enabled()`
        // or have an `Scheme::associated_builder` that given a scheme returns the associated builder? The problem with this
        // is that `Scheme` variants are not gate behind a feature gate and the associated builder is. As a workaround

        // TODO: it seems too error-prone to have this list manually updated, we should have a macro that generates this list?
        // it could be something like:
        //
        // ```rust
        // apply_for_all_services!{
        //    registry.register_builder::<$service>();
        // }
        // ```
        // and the apply_for_all_services macro would gate every statement behind the corresponding feature gate
        // This seems to not be the place where we should have a "list of enabled services".
        // Ther is something similar with `Scheme::enabled()`
        #[cfg(feature = "services-aliyun-drive")]
        registry.register_builder::<AliyunDrive>();
        #[cfg(feature = "services-atomicserver")]
        registry.register_builder::<Atomicserver>();
        #[cfg(feature = "services-alluxio")]
        registry.register_builder::<Alluxio>();
        #[cfg(feature = "services-azblob")]
        registry.register_builder::<Azblob>();
        #[cfg(feature = "services-azdls")]
        registry.register_builder::<Azdls>();
        #[cfg(feature = "services-azfile")]
        registry.register_builder::<Azfile>();
        #[cfg(feature = "services-b2")]
        registry.register_builder::<B2>();
        #[cfg(feature = "services-cacache")]
        registry.register_builder::<Cacache>();
        #[cfg(feature = "services-cos")]
        registry.register_builder::<Cos>();
        #[cfg(feature = "services-compfs")]
        registry.register_builder::<Compfs>();
        #[cfg(feature = "services-dashmap")]
        registry.register_builder::<Dashmap>();
        #[cfg(feature = "services-dropbox")]
        registry.register_builder::<Dropbox>();
        #[cfg(feature = "services-etcd")]
        registry.register_builder::<Etcd>();
        #[cfg(feature = "services-foundationdb")]
        registry.register_builder::<Foundationdb>();
        #[cfg(feature = "services-fs")]
        registry.register_builder::<Fs>();
        #[cfg(feature = "services-ftp")]
        registry.register_builder::<Ftp>();
        #[cfg(feature = "services-gcs")]
        registry.register_builder::<Gcs>();
        #[cfg(feature = "services-ghac")]
        registry.register_builder::<Ghac>();
        #[cfg(feature = "services-hdfs")]
        registry.register_builder::<Hdfs>();
        #[cfg(feature = "services-http")]
        registry.register_builder::<Http>();
        #[cfg(feature = "services-huggingface")]
        registry.register_builder::<Huggingface>();
        #[cfg(feature = "services-ipfs")]
        registry.register_builder::<Ipfs>();
        #[cfg(feature = "services-ipmfs")]
        registry.register_builder::<Ipmfs>();
        #[cfg(feature = "services-icloud")]
        registry.register_builder::<Icloud>();
        #[cfg(feature = "services-libsql")]
        registry.register_builder::<Libsql>();
        #[cfg(feature = "services-memcached")]
        registry.register_builder::<Memcached>();
        #[cfg(feature = "services-memory")]
        registry.register_builder::<Memory>();
        #[cfg(feature = "services-mini-moka")]
        registry.register_builder::<MiniMoka>();
        #[cfg(feature = "services-moka")]
        registry.register_builder::<Moka>();
        #[cfg(feature = "services-monoiofs")]
        registry.register_builder::<Monoiofs>();
        #[cfg(feature = "services-mysql")]
        registry.register_builder::<Mysql>();
        #[cfg(feature = "services-obs")]
        registry.register_builder::<Obs>();
        #[cfg(feature = "services-onedrive")]
        registry.register_builder::<Onedrive>();
        #[cfg(feature = "services-postgresql")]
        registry.register_builder::<Postgresql>();
        #[cfg(feature = "services-gdrive")]
        registry.register_builder::<Gdrive>();
        #[cfg(feature = "services-oss")]
        registry.register_builder::<Oss>();
        #[cfg(feature = "services-persy")]
        registry.register_builder::<Persy>();
        #[cfg(feature = "services-redis")]
        registry.register_builder::<Redis>();
        #[cfg(feature = "services-rocksdb")]
        registry.register_builder::<Rocksdb>();
        #[cfg(feature = "services-s3")]
        registry.register_builder::<S3>();
        #[cfg(feature = "services-seafile")]
        registry.register_builder::<Seafile>();
        #[cfg(feature = "services-upyun")]
        registry.register_builder::<Upyun>();
        #[cfg(feature = "services-yandex-disk")]
        registry.register_builder::<YandexDisk>();
        #[cfg(feature = "services-pcloud")]
        registry.register_builder::<Pcloud>();
        #[cfg(feature = "services-sftp")]
        registry.register_builder::<Sftp>();
        #[cfg(feature = "services-sled")]
        registry.register_builder::<Sled>();
        #[cfg(feature = "services-sqlite")]
        registry.register_builder::<Sqlite>();
        #[cfg(feature = "services-supabase")]
        registry.register_builder::<Supabase>();
        #[cfg(feature = "services-swift")]
        registry.register_builder::<Swift>();
        #[cfg(feature = "services-tikv")]
        registry.register_builder::<Tikv>();
        #[cfg(feature = "services-vercel-artifacts")]
        registry.register_builder::<VercelArtifacts>();
        #[cfg(feature = "services-vercel-blob")]
        registry.register_builder::<VercelBlob>();
        #[cfg(feature = "services-webdav")]
        registry.register_builder::<Webdav>();
        #[cfg(feature = "services-webhdfs")]
        registry.register_builder::<Webhdfs>();
        #[cfg(feature = "services-redb")]
        registry.register_builder::<Redb>();
        #[cfg(feature = "services-mongodb")]
        registry.register_builder::<Mongodb>();
        #[cfg(feature = "services-hdfs-native")]
        registry.register_builder::<HdfsNative>();
        #[cfg(feature = "services-surrealdb")]
        registry.register_builder::<Surrealdb>();
        #[cfg(feature = "services-lakefs")]
        registry.register_builder::<Lakefs>();
        #[cfg(feature = "services-nebula-graph")]
        registry.register_builder::<NebulaGraph>();

        registry
    }

    fn register_builder<B: Builder>(&mut self) {
        self.register(
            B::SCHEME.into_static(),
            operator_factory_from_configurator::<B::Config>(),
        );
    }
}

fn operator_factory_from_configurator<C: Configurator>() -> OperatorFactory {
    |uri, options| {
        let builder = C::from_uri(uri, options)?.into_builder();
        Ok(Operator::new(builder)?.finish())
    }
}
