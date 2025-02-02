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

use std::cell::RefCell;

use magnus::class;
use magnus::method;
use magnus::prelude::*;
use magnus::Error;
use magnus::RModule;
use magnus::Ruby;
use magnus::Value;

use crate::metadata::Metadata;
use crate::*;

/// Represents the result when list a directory
#[magnus::wrap(class = "OpenDAL::Lister", free_immediately, size)]
pub struct Lister(RefCell<ocore::BlockingLister>);

/// Entry returned by Lister to represent a path and it's relative metadata.
#[magnus::wrap(class = "OpenDAL::Entry", free_immediately, size)]
pub struct Entry(ocore::Entry);

impl Entry {
    /// Gets the path of entry. Path is relative to operator's root.
    ///
    /// Only valid in current operator.
    ///
    /// If this entry is a dir, `path` MUST end with `/`
    /// Otherwise, `path` MUST NOT end with `/`.
    fn path(&self) -> Result<&str, Error> {
        Ok(self.0.path())
    }

    /// Gets the name of entry. Name is the last segment of path.
    ///
    /// If this entry is a dir, `name` MUST end with `/`
    /// Otherwise, `name` MUST NOT end with `/`.
    fn name(&self) -> Result<&str, Error> {
        Ok(self.0.name())
    }

    /// Fetches the metadata of this entry.
    fn metadata(&self) -> Result<Metadata, Error> {
        Ok(Metadata::new(self.0.metadata().clone()))
    }
}

impl Lister {
    /// Creates a new blocking Lister.
    pub fn new(inner: ocore::BlockingLister) -> Self {
        Self(RefCell::new(inner))
    }

    /// Returns the next element.
    fn each(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        while let Some(Ok(entry)) = rb_self.0.borrow_mut().next() {
            // we don't need the return value of the yield block
            let _ = ruby.yield_value::<lister::Entry, Value>(Entry(entry));
        }
        Ok(())
    }
}

pub fn include(ruby: &Ruby, gem_module: &RModule) -> Result<(), Error> {
    let entry_class = gem_module.define_class("Entry", class::object())?;
    entry_class.define_method("path", method!(Entry::path, 0))?;
    entry_class.define_method("name", method!(Entry::name, 0))?;
    entry_class.define_method("metadata", method!(Entry::metadata, 0))?;

    let lister_class = gem_module.define_class("Lister", class::object())?;
    let _ = lister_class
        .include_module(ruby.module_enumerable())
        .map_err(|err| Error::new(ruby.exception_runtime_error(), err.to_string()))?;
    lister_class.define_method("each", method!(Lister::each, 0))?;

    Ok(())
}
