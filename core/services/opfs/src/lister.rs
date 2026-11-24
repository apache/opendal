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

#[cfg(target_arch = "wasm32")]
use std::sync::Arc;

#[cfg(target_arch = "wasm32")]
use send_wrapper::SendWrapper;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;

#[cfg(target_arch = "wasm32")]
use super::core::OpfsCore;
#[cfg(target_arch = "wasm32")]
use super::core::*;
#[cfg(target_arch = "wasm32")]
use opendal_core::raw::*;
#[cfg(target_arch = "wasm32")]
use opendal_core::*;

pub(crate) fn normalize_list_prefix(path: &str) -> String {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}/")
    }
}

#[cfg(target_arch = "wasm32")]
pub struct OpfsLister {
    core: Arc<OpfsCore>,
    iter: Option<SendWrapper<js_sys::AsyncIterator>>,
    path: String,
    done: bool,
}

#[cfg(target_arch = "wasm32")]
impl OpfsLister {
    pub fn new(core: Arc<OpfsCore>, path: String) -> Self {
        // Entry paths must not start with '/', and non-empty directory prefixes
        // must end with '/' so child names are separated properly.
        let path = normalize_list_prefix(&path);
        Self {
            core,
            iter: None,
            path,
            done: false,
        }
    }

    async fn init_iter(&mut self) -> Result<bool> {
        if self.iter.is_some() {
            return Ok(true);
        }

        let p = build_absolute_path(&self.core.root, &self.path);
        match get_directory_handle(&p, false).await {
            Ok(dir) => {
                self.iter = Some(SendWrapper::new(dir.entries()));
                Ok(true)
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                self.done = true;
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl oio::List for OpfsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.done {
            return Ok(None);
        }
        if !self.init_iter().await? {
            return Ok(None);
        }

        let iter = self
            .iter
            .as_ref()
            .expect("opfs list iterator must be initialized");
        let result = JsFuture::from(iter.next().map_err(parse_js_error)?)
            .await
            .map_err(parse_js_error)?;

        let done = js_sys::Reflect::get(&result, &"done".into())
            .unwrap_or(JsValue::TRUE)
            .as_bool()
            .unwrap_or(true);
        if done {
            self.done = true;
            return Ok(None);
        }

        let value = js_sys::Reflect::get(&result, &"value".into()).map_err(parse_js_error)?;
        let pair: js_sys::Array = value.unchecked_into();
        let name = pair.get(0).as_string().unwrap_or_default();
        let handle = pair.get(1);

        let kind = js_sys::Reflect::get(&handle, &"kind".into())
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let (entry_path, metadata) = if kind == "directory" {
            (format!("{}{}/", self.path, name), MetadataBuilder::dir())
        } else {
            (format!("{}{}", self.path, name), MetadataBuilder::unknown())
        };

        Ok(Some(oio::Entry::new(&entry_path, metadata.build())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_list_prefix_handles_root_and_trailing_slash() {
        assert_eq!(normalize_list_prefix(""), "");
        assert_eq!(normalize_list_prefix("/"), "");
        assert_eq!(normalize_list_prefix("///"), "");
        assert_eq!(normalize_list_prefix("sub"), "sub/");
        assert_eq!(normalize_list_prefix("sub/"), "sub/");
        assert_eq!(normalize_list_prefix("/sub"), "sub/");
        assert_eq!(normalize_list_prefix("/sub/"), "sub/");
        assert_eq!(normalize_list_prefix("a/b"), "a/b/");
        assert_eq!(normalize_list_prefix("a/b/"), "a/b/");
    }

    #[test]
    fn normalized_prefix_formats_child_entries_with_separator() {
        let prefix = normalize_list_prefix("sub");
        assert_eq!(format!("{prefix}file"), "sub/file");
        assert_eq!(format!("{prefix}child/"), "sub/child/");

        let root_prefix = normalize_list_prefix("/");
        assert_eq!(format!("{root_prefix}file"), "file");
        assert_eq!(format!("{root_prefix}child/"), "child/");
    }
}
