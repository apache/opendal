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

use crate::Result;
use crate::{
    raw::{normalize_path, output},
    ObjectMetadata, ObjectMode,
};
use async_trait::async_trait;

use super::list_response::Multistatus;
use std::path::{Path, PathBuf};

pub struct DirStream {
    root: PathBuf,

    size: usize,
    multistates: Multistatus,
}

impl DirStream {
    pub fn new(root: &Path, multistates: Multistatus, limit: Option<usize>) -> Self {
        Self {
            root: root.to_owned(),
            size: limit.unwrap_or(1000),
            multistates: multistates,
        }
    }
}

#[async_trait]
impl output::Page for DirStream {
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        let mut oes: Vec<output::Entry> = Vec::with_capacity(self.size);

        for i in 0..self.size {
            if i >= self.multistates.response.len() {
                break;
            }
            match self.multistates.response.get(0) {
                Some(de) => {
                    let path = PathBuf::from(de.href.clone());

                    let rel_path = normalize_path(
                        &path
                            .strip_prefix(&self.root)
                            .expect("cannot fail because the prefix is iterated")
                            .to_string_lossy()
                            .replace('\\', "/"),
                    );

                    let entry = if de.propstat.prop.resourcetype.value
                        == Some(super::list_response::ResourceType::Collection)
                    {
                        output::Entry::new(&rel_path, ObjectMetadata::new(ObjectMode::DIR))
                    } else {
                        output::Entry::new(&rel_path, ObjectMetadata::new(ObjectMode::FILE))
                    };

                    oes.push(entry);
                }
                None => break,
            }
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}
