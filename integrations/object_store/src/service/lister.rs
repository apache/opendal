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

use std::sync::Arc;

use futures::StreamExt;
use futures::stream;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use opendal::raw::*;
use opendal::*;

use super::error::parse_error;
use crate::service::core::format_metadata;

pub struct ObjectStoreLister {
    stream: BoxStream<'static, Result<oio::Entry>>,
}

impl ObjectStoreLister {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore + 'static>,
        path: &str,
        args: OpList,
    ) -> Result<Self> {
        // `OpList::limit` is a per-page size hint. object_store does not expose
        // a page-size parameter, so the stream must return every result and
        // ignore the hint rather than truncating the listing.
        let stream = if args.recursive() {
            recursive_stream(store, path, args.start_after())
        } else {
            non_recursive_stream(store, path, args.start_after())
        };

        Ok(Self { stream })
    }
}

/// Build a recursive listing.
///
/// [`ObjectStore::list`] is always recursive. A path without a trailing `/` is
/// still a prefix, so list its parent and keep only entries that start with the
/// requested prefix.
///
/// `object_store::Path` stores percent-encoded locations, while OpenDAL paths
/// are decoded. Decode locations before comparing them with the requested
/// prefix or `start_after`, and return the decoded path in the entry.
fn recursive_stream(
    store: Arc<dyn ObjectStore + 'static>,
    path: &str,
    start_after: Option<&str>,
) -> BoxStream<'static, Result<oio::Entry>> {
    let (directory, prefix) = if path == "/" || path.ends_with('/') {
        (path, path.trim_start_matches('/').to_string())
    } else {
        (get_parent(path), path.to_string())
    };
    let start_after = start_after.map(str::to_string);

    // Do not use `list_with_offset`: it compares percent-encoded locations,
    // whose order can differ from the decoded OpenDAL order. Filter after
    // decoding so `start_after` keeps OpenDAL semantics.
    store
        .list(Some(&ObjectStorePath::from(directory)))
        .filter_map(move |item| {
            let entry = match item {
                Ok(meta) => {
                    let location = percent_decode_path(meta.location.as_ref());
                    if !location.starts_with(&prefix)
                        || start_after
                            .as_deref()
                            .is_some_and(|marker| location.as_str() <= marker)
                    {
                        return futures::future::ready(None);
                    }

                    let metadata = format_metadata(&meta);
                    Ok(oio::Entry::new(&location, metadata))
                }
                // Local filesystem stores report a missing directory as NotFound.
                // Listing a missing prefix is empty in OpenDAL.
                Err(err) if matches!(&err, object_store::Error::NotFound { .. }) => {
                    return futures::future::ready(None);
                }
                Err(err) => Err(parse_error(err)),
            };

            futures::future::ready(Some(entry))
        })
        .boxed()
}

/// Build a non-recursive listing from [`ObjectStore::list_with_delimiter`].
///
/// The delimiter API returns direct children and folds deeper objects into
/// common prefixes. A path without a trailing `/` is still a prefix, so list
/// its parent and keep only entries that start with the requested prefix.
///
/// Common prefixes and object locations are percent-encoded by
/// `object_store::Path`, so decode them before returning entries and comparing
/// them with the OpenDAL prefix or `start_after`.
fn non_recursive_stream(
    store: Arc<dyn ObjectStore + 'static>,
    path: &str,
    start_after: Option<&str>,
) -> BoxStream<'static, Result<oio::Entry>> {
    let (directory, prefix) = if path == "/" || path.ends_with('/') {
        (path, None)
    } else {
        (get_parent(path), Some(path.to_string()))
    };
    let directory = ObjectStorePath::from(directory);
    let start_after = start_after.map(str::to_string);

    stream::once(async move {
        let result = match store.list_with_delimiter(Some(&directory)).await {
            Ok(result) => result,
            // Local filesystem stores report a missing directory as NotFound.
            // Listing a missing prefix is empty in OpenDAL.
            Err(err) if matches!(&err, object_store::Error::NotFound { .. }) => {
                return Vec::new();
            }
            Err(err) => return vec![Err(parse_error(err))],
        };

        let mut entries: Vec<Result<oio::Entry>> =
            Vec::with_capacity(result.common_prefixes.len() + result.objects.len());
        for common_prefix in result.common_prefixes {
            let entry_path = format!("{}/", percent_decode_path(common_prefix.as_ref()));
            if !keep_entry(&entry_path, prefix.as_deref(), start_after.as_deref()) {
                continue;
            }
            entries.push(Ok(oio::Entry::new(
                &entry_path,
                MetadataBuilder::dir().build(),
            )));
        }
        for meta in result.objects {
            let entry_path = percent_decode_path(meta.location.as_ref());
            if !keep_entry(&entry_path, prefix.as_deref(), start_after.as_deref()) {
                continue;
            }
            entries.push(Ok(oio::Entry::new(&entry_path, format_metadata(&meta))));
        }
        entries
    })
    .flat_map(stream::iter)
    .boxed()
}

fn keep_entry(path: &str, prefix: Option<&str>, start_after: Option<&str>) -> bool {
    prefix.is_none_or(|prefix| path.starts_with(prefix))
        && start_after.is_none_or(|marker| path > marker)
}

// ObjectStoreLister is safe to share between threads, because it only has &mut self API calls.
unsafe impl Sync for ObjectStoreLister {}

impl oio::List for ObjectStoreLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.stream.next().await.transpose()
    }
}
