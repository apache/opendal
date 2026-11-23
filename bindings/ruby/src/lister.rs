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

#![allow(
    rustdoc::broken_intra_doc_links,
    reason = "YARD's syntax for documentation"
)]
#![allow(rustdoc::invalid_html_tags, reason = "YARD's syntax for documentation")]
#![allow(rustdoc::bare_urls, reason = "YARD's syntax for documentation")]

use std::sync::Arc;
use std::sync::Mutex;

use magnus::Error;
use magnus::RModule;
use magnus::Ruby;
use magnus::TryConvert;
use magnus::Value;
use magnus::method;
use magnus::prelude::*;

use crate::metadata::Metadata;
use crate::*;

/// @yard
/// Entry returned by Lister to represent a path and it's relative metadata.
#[magnus::wrap(class = "OpenDal::Entry", free_immediately, size)]
pub struct Entry(ocore::Entry);

impl Entry {
    /// @yard
    /// @def path
    /// Gets the path of entry. Path is relative to operator's root.
    ///
    /// Only valid in current operator.
    ///
    /// If this entry is a dir, `path` MUST end with `/`
    /// Otherwise, `path` MUST NOT end with `/`.
    /// @return [String]
    fn path(&self) -> Result<&str, Error> {
        Ok(self.0.path())
    }

    /// @yard
    /// @def name
    /// Gets the name of entry. Name is the last segment of path.
    ///
    /// If this entry is a dir, `name` MUST end with `/`
    /// Otherwise, `name` MUST NOT end with `/`.
    /// @return [String]
    fn name(&self) -> Result<&str, Error> {
        Ok(self.0.name())
    }

    /// @yard
    /// @def metadata
    /// Fetches the metadata of this entry.
    /// @return [Metadata]
    fn metadata(&self) -> Result<Metadata, Error> {
        Ok(Metadata::new(self.0.metadata().clone()))
    }
}

/// @yard
/// Represents the result when list a directory
///
/// This class is an enumerable.
///
/// # Safety
///
/// `Lister` is thread-safe.
#[magnus::wrap(class = "OpenDal::Lister", free_immediately, size)]
pub struct Lister(Arc<Mutex<ocore::blocking::Lister>>);

impl Lister {
    /// Creates a new blocking Lister.
    pub fn new(inner: ocore::blocking::Lister) -> Self {
        Self(Arc::new(Mutex::new(inner)))
    }

    /// @yard
    /// @def each
    /// Yields each entry to the block, or returns an Enumerator when no block
    /// is given.
    /// @return [Enumerator, Lister]
    /// @raise [RuntimeError] when listing fails on the underlying storage
    fn each(ruby: &Ruby, rb_self: Value) -> Result<Value, Error> {
        if !ruby.block_given() {
            return Ok(rb_self.enumeratorize("each", ()).as_value());
        }

        let lister: &Self = TryConvert::try_convert(rb_self)?;
        loop {
            // Hold the lock only while pulling the next entry so that a `break`
            // or `return` inside the block never leaves the mutex locked.
            //
            // `each` may be called from multiple threads or fibers, so the
            // underlying lister is wrapped in `Arc<Mutex<_>>`. A poisoned lock
            // means another fiber panicked while iterating; surface that as an
            // error rather than silently ending the iteration.
            let item = lister
                .0
                .lock()
                .map_err(|_| {
                    Error::new(
                        ruby.exception_runtime_error(),
                        "lister lock poisoned by another fiber",
                    )
                })?
                .next();
            match item {
                Some(Ok(entry)) => {
                    let _: Value = ruby.yield_value(Entry(entry))?;
                }
                // A storage error is not end-of-iteration: propagate it so it
                // raises instead of silently returning a truncated listing.
                Some(Err(err)) => return Err(format_magnus_error(ruby, err)),
                None => break,
            }
        }
        Ok(rb_self)
    }
}

pub fn include(ruby: &Ruby, gem_module: &RModule) -> Result<(), Error> {
    let entry_class = gem_module.define_class("Entry", ruby.class_object())?;
    entry_class.define_method("path", method!(Entry::path, 0))?;
    entry_class.define_method("name", method!(Entry::name, 0))?;
    entry_class.define_method("metadata", method!(Entry::metadata, 0))?;

    let lister_class = gem_module.define_class("Lister", ruby.class_object())?;
    lister_class
        .include_module(ruby.module_enumerable())
        .map_err(|err| Error::new(ruby.exception_runtime_error(), err.to_string()))?;
    lister_class.define_method("each", method!(Lister::each, 0))?;

    Ok(())
}
