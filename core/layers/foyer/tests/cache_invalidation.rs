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

use foyer::{HybridCache, HybridCacheBuilder, RecoverMode};
use futures::StreamExt;
use opendal_core::raw::{Service, oio};
use opendal_core::{ErrorKind, Operator};
use opendal_layer_foyer::{Deleter, FoyerKey, FoyerLayer, FoyerService, FoyerValue, Writer};
use opendal_service_fs::Fs;

async fn cached_fs(dir: &tempfile::TempDir) -> (Operator, HybridCache<FoyerKey, FoyerValue>) {
    let cache = HybridCacheBuilder::new()
        .memory(1024 * 1024)
        .with_shards(1)
        .storage()
        .with_recover_mode(RecoverMode::None)
        .build()
        .await
        .unwrap();
    let op = Operator::new(Fs::default().root(dir.path().to_str().unwrap()))
        .unwrap()
        .layer(FoyerLayer::new(cache.clone()));
    (op, cache)
}

fn unversioned_key(path: &str) -> FoyerKey {
    FoyerKey {
        path: path.to_string(),
        version: None,
    }
}

#[tokio::test]
async fn rename_invalidates_source_and_destination() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let source = unversioned_key("source");
    let destination = unversioned_key("destination");
    op.write(&source.path, "original").await.unwrap();
    op.write(&destination.path, "stale").await.unwrap();
    assert!(cache.contains(&source));
    assert!(cache.contains(&destination));

    op.rename(&source.path, &destination.path).await.unwrap();

    assert_eq!(
        op.read(&source.path).await.unwrap_err().kind(),
        ErrorKind::NotFound,
    );
    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"original",
    );
    assert!(!cache.contains(&source));
    assert!(!cache.contains(&destination));

    op.write(&destination.path, "new write").await.unwrap();
    assert!(cache.contains(&destination));
    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"new write",
    );
    op.delete(&destination.path).await.unwrap();
    assert!(!cache.contains(&destination));
    assert_eq!(
        op.read(&destination.path).await.unwrap_err().kind(),
        ErrorKind::NotFound,
    );
}

#[tokio::test]
async fn operator_copy_invalidates_destination_only() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let source = unversioned_key("source");
    let destination = unversioned_key("destination");
    op.write(&source.path, "original").await.unwrap();
    op.write(&destination.path, "stale").await.unwrap();
    assert!(cache.contains(&source));
    assert!(cache.contains(&destination));

    let metadata = op.copy(&source.path, &destination.path).await.unwrap();

    assert_eq!(metadata.content_length(), 8);
    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"original",
    );
    assert!(cache.contains(&source));
    assert!(!cache.contains(&destination));
}

#[tokio::test]
async fn copier_stream_invalidates_destination_at_end_of_stream() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let destination = unversioned_key("destination");
    op.write("source", "original").await.unwrap();
    op.write(&destination.path, "stale").await.unwrap();

    let mut copier = op.copier("source", &destination.path).await.unwrap();
    assert!(cache.contains(&destination));
    assert!(
        StreamExt::next(&mut copier)
            .await
            .transpose()
            .unwrap()
            .is_none()
    );

    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"original",
    );
    assert!(!cache.contains(&destination));

    op.write(&destination.path, "new write").await.unwrap();
    assert_eq!(copier.close().await.unwrap().content_length(), 8);
    assert!(cache.contains(&destination));
}

#[tokio::test]
async fn failed_mutations_and_aborted_copy_keep_cached_destination() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let destination = unversioned_key("destination");
    op.write(&destination.path, "cached").await.unwrap();

    let err = op.rename("missing", &destination.path).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(cache.contains(&destination));
    let err = op.copy("missing", &destination.path).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(cache.contains(&destination));

    op.write("source", "original").await.unwrap();
    let mut copier = op.copier("source", &destination.path).await.unwrap();
    copier.abort().await.unwrap();
    assert!(cache.contains(&destination));
    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"cached"
    );
}

#[test]
fn operation_types_remain_available() {
    fn assert_operation_types<
        S: Service<
                Deleter = Deleter<oio::Deleter>,
                Writer = Writer<oio::Writer>,
                Copier = oio::Copier,
            >,
    >() {
    }

    assert_operation_types::<FoyerService>();
}
