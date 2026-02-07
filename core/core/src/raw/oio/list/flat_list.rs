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

/// FlatLister will walk dir in bottom up way:
///
/// - List nested dir first
/// - Go back into parent dirs one by one
///
/// Given the following file tree:
///
/// ```txt
/// .
/// ├── dir_x/
/// │   ├── dir_y/
/// │   │   ├── dir_z/
/// │   │   └── file_c
/// │   └── file_b
/// └── file_a
/// ```
///
/// ToFlatLister will output entries like:
///
/// ```txt
/// dir_x/dir_y/dir_z/file_c
/// dir_x/dir_y/dir_z/
/// dir_x/dir_y/file_b
/// dir_x/dir_y/
/// dir_x/file_a
/// dir_x/
/// ```
///
/// # Note
///
/// There is no guarantee about the order between files and dirs at the same level.
/// We only make sure the nested dirs will show up before parent dirs.
///
/// Especially, for storage services that can't return dirs first, ToFlatLister
/// may output parent dirs' files before nested dirs, this is expected because files
/// always output directly while listing.
pub struct FlatLister<A: Access, L> {
    acc: A,

    next_dir: Option<oio::Entry>,
    active_lister: Vec<(Option<oio::Entry>, L)>,
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this FlatLister.
unsafe impl<A: Access, L> Send for FlatLister<A, L> {}
/// # Safety
///
/// We will only take `&mut Self` reference for FsLister.
unsafe impl<A: Access, L> Sync for FlatLister<A, L> {}

impl<A, L> FlatLister<A, L>
where
    A: Access,
{
    /// Create a new flat lister
    pub fn new(acc: A, path: &str) -> FlatLister<A, L> {
        FlatLister {
            acc,
            next_dir: Some(oio::Entry::new(path, Metadata::new(EntryMode::DIR))),
            active_lister: vec![],
        }
    }
}

impl<A, L> oio::List for FlatLister<A, L>
where
    A: Access<Lister = L>,
    L: oio::List,
{
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            if let Some(de) = self.next_dir.take() {
                let (_, mut l) = match self.acc.list(de.path(), OpList::new()).await {
                    Ok(v) => v,
                    Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                        // Skip directories that we don't have permission to access
                        // and continue with the rest of the listing.
                        log::warn!(
                            "FlatLister skipping directory due to permission denied: {}",
                            de.path()
                        );
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                if let Some(v) = l.next().await? {
                    self.active_lister.push((Some(de.clone()), l));

                    if v.mode().is_dir() {
                        // should not loop itself again
                        if v.path() != de.path() {
                            self.next_dir = Some(v);
                            continue;
                        }
                    } else {
                        return Ok(Some(v));
                    }
                }
            }

            let (de, lister) = match self.active_lister.last_mut() {
                Some((de, lister)) => (de, lister),
                None => return Ok(None),
            };

            match lister.next().await? {
                Some(v) if v.mode().is_dir() => {
                    // should not loop itself again
                    if v.path() != de.as_ref().expect("de should not be none here").path() {
                        self.next_dir = Some(v);
                        continue;
                    }
                }
                Some(v) => return Ok(Some(v)),
                None => match de.take() {
                    Some(de) => {
                        return Ok(Some(de));
                    }
                    None => {
                        let _ = self.active_lister.pop();
                        continue;
                    }
                },
            }
        }
    }
}
