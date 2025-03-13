use serde::Deserialize;
use std::sync::Arc;

use super::{core::OpfsCore, lister::OpfsLister, reader::OpfsReader, writer::OpfsWriter};
use crate::{
    raw::{
        Access, AccessorInfo, OpCopy, OpCreateDir, OpDelete, OpList, OpRead, OpRename, OpStat,
        OpWrite, RpCopy, RpCreateDir, RpDelete, RpList, RpRead, RpRename, RpStat, RpWrite,
    },
    types, Builder, Capability, Error, Result, Scheme,
};
use std::fmt::Debug;
use wasm_bindgen_futures::spawn_local;

/// Origin private file system (OPFS) configuration
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct OpfsConfig {}

impl Debug for OpfsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!()
    }
}

/// Origin private file system (OPFS) support
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct OpfsBuilder {
    config: OpfsConfig,
}

impl OpfsBuilder {}

impl Debug for OpfsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!()
    }
}

impl Builder for OpfsBuilder {
    const SCHEME: Scheme = Scheme::Opfs;

    type Config = ();

    fn build(self) -> Result<impl Access> {
        Ok(OpfsBackend {})
    }
}

/// OPFS Service backend
#[derive(Debug, Clone)]
pub struct OpfsBackend {}

impl Access for OpfsBackend {
    type Reader = OpfsReader;

    type Writer = OpfsWriter;

    type Lister = OpfsLister;

    type Deleter = ();

    type BlockingReader = OpfsReader;

    type BlockingWriter = OpfsWriter;

    type BlockingLister = OpfsLister;

    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let access_info = AccessorInfo::default();
        access_info
            .set_scheme(Scheme::Opfs)
            .set_native_capability(Capability {
                stat: false,
                read: true,
                write: true,
                write_can_empty: true,
                write_can_append: true,
                write_can_multi: true,
                create_dir: false,
                delete: false,
                list: false,
                copy: false,
                rename: false,
                blocking: true,
                ..Default::default()
            });
        Arc::new(access_info)
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = path.to_owned();

        // OpfsHelper::read_file_with_local_set(path).await;
        let out_buf = OpfsCore::read_file(path.as_str()).await?;
        Ok::<(RpRead, Self::Reader), Error>((RpRead::default(), Self::Reader {}))
    }
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        // Access the OPFS
        let path = path.to_owned();

        spawn_local(async move {
            // panic won't return
            OpfsCore::store_file(path.as_str(), &[1, 2, 3, 4])
                .await
                .expect("store file error");
        })
        .await
        .unwrap()?;

        Ok((RpWrite::default(), Self::Writer {}))
    }
}

#[cfg(test)]
#[cfg(target_arch = "wasm32")]
mod opfs_tests {
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_test::*;

    use std::collections::HashMap;

    use crate::Operator;

    use super::*;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen]
    pub async fn test_opfs() -> String {
        let map = HashMap::new();
        let op = Operator::via_map(Scheme::Opfs, map).unwrap();
        let bs = op.read("path/to/file").await.unwrap();
        "ok".to_string()
    }

    #[wasm_bindgen_test]
    async fn basic_test() -> Result<()> {
        let s = test_opfs().await;
        assert_eq!(s, "ok".to_string());
        Ok(())
    }
}
