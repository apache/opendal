// Copyright 2023 Datafuse Labs.
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

use async_trait::async_trait;

use crate::raw::*;
use crate::*;

/// to_hierarchy_pager is used to make a hierarchy pager flat.
pub fn to_hierarchy_pager<P: output::Page>(pager: P, path: &str) -> ToHierarchyPager<P> {
    ToHierarchyPager {
        pager,
        path: path.to_string(),
    }
}

/// ToHierarchyPager will convert a flat page to hierarchy by filter
/// not needed entires.
///
/// # Notes
///
/// ToHierarchyPager filter entries after fecth entries. So it's possible
/// to return an empty vec. It doesn't mean the all pages have been
/// returned.
///
/// Please keep calling next_page until we returned `Ok(None)`
pub struct ToHierarchyPager<P: output::Page> {
    pager: P,
    path: String,
}

#[async_trait]
impl<P: output::Page> output::Page for ToHierarchyPager<P> {
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        let page = self.pager.next_page().await?;

        let mut entries = if let Some(entries) = page {
            entries
        } else {
            return Ok(None);
        };

        entries.retain(|e| {
            let path = if let Some(path) = e.path().strip_prefix(&self.path) {
                path
            } else {
                // If path is not started with prefix, drop it.
                //
                // Idealy, it should never happen. But we just tolerate
                // this state.
                return false;
            };

            let idx = if let Some(idx) = path.find('/') {
                idx
            } else {
                // If there is no `/` in path, it's a normal file, we
                // can return it directly.
                return true;
            };

            // idx == path.len() means it's contain only one `/` at the
            // end of path.
            idx == path.len()
        });

        Ok(Some(entries))
    }
}
