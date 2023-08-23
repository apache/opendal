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

//! Functions provides the functions generated by [`BlockingOperator`]
//!
//! By using functions, users can add more options for operation.

use std::ops::RangeBounds;

use bytes::Bytes;
use flagset::FlagSet;

use crate::raw::*;
use crate::*;

/// OperatorFunction is the function generated by [`BlockingOperator`].
///
/// The function will consume all the input to generate a result.
pub(crate) struct OperatorFunction<T, R> {
    inner: FusedAccessor,
    path: String,
    args: T,
    f: fn(FusedAccessor, String, T) -> Result<R>,
}

impl<T, R> OperatorFunction<T, R> {
    pub fn new(
        inner: FusedAccessor,
        path: String,
        args: T,
        f: fn(FusedAccessor, String, T) -> Result<R>,
    ) -> Self {
        Self {
            inner,
            path,
            args,
            f,
        }
    }

    fn map_args(self, f: impl FnOnce(T) -> T) -> Self {
        Self {
            inner: self.inner,
            path: self.path,
            args: f(self.args),
            f: self.f,
        }
    }

    fn call(self) -> Result<R> {
        (self.f)(self.inner, self.path, self.args)
    }
}

/// Function that generated by [`BlockingOperator::write_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FunctionWrite(
    /// The args for FunctionWrite is a bit special because we also
    /// need to move the bytes input this function.
    pub(crate) OperatorFunction<(OpWrite, Bytes), ()>,
);

impl FunctionWrite {
    /// Set the content length for this operation.
    pub fn content_length(mut self, v: u64) -> Self {
        self.0 = self
            .0
            .map_args(|(args, bs)| (args.with_content_length(v), bs));
        self
    }

    /// Set the content type for this operation.
    pub fn content_type(mut self, v: &str) -> Self {
        self.0 = self
            .0
            .map_args(|(args, bs)| (args.with_content_type(v), bs));
        self
    }

    /// Call the function to consume all the input and generate a
    /// result.
    pub fn call(self) -> Result<()> {
        self.0.call()
    }
}

/// Function that generated by [`BlockingOperator::delete_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FunctionDelete(pub(crate) OperatorFunction<OpDelete, ()>);

impl FunctionDelete {
    /// Set the version for this operation.
    pub fn version(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_version(v));
        self
    }

    /// Call the function to consume all the input and generate a
    /// result.
    pub fn call(self) -> Result<()> {
        self.0.call()
    }
}

/// Function that generated by [`BlockingOperator::list_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FunctionList(pub(crate) OperatorFunction<OpList, Vec<Entry>>);

impl FunctionList {
    /// Change the limit of this list operation.
    pub fn limit(mut self, v: usize) -> Self {
        self.0 = self.0.map_args(|args| args.with_limit(v));
        self
    }

    /// Change the start_after of this list operation.
    pub fn start_after(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_start_after(v));
        self
    }

    /// Change the delimiter. The default delimiter is "/"
    pub fn delimiter(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_delimiter(v));
        self
    }

    /// Change the metakey. The default metakey is `Metakey::Mode`.
    pub fn metakey(mut self, v: impl Into<FlagSet<Metakey>>) -> Self {
        self.0 = self.0.map_args(|args| args.with_metakey(v));
        self
    }

    /// Call the function to consume all the input and generate a
    /// result.
    pub fn call(self) -> Result<Vec<Entry>> {
        self.0.call()
    }
}

/// Function that generated by [`BlockingOperator::lister_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FunctionLister(pub(crate) OperatorFunction<OpList, BlockingLister>);

impl FunctionLister {
    /// Change the limit of this list operation.
    pub fn limit(mut self, v: usize) -> Self {
        self.0 = self.0.map_args(|args| args.with_limit(v));
        self
    }

    /// Change the start_after of this list operation.
    pub fn start_after(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_start_after(v));
        self
    }

    /// Change the delimiter. The default delimiter is "/"
    pub fn delimiter(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_delimiter(v));
        self
    }

    /// Change the metakey. The default metakey is `Metakey::Mode`.
    pub fn metakey(mut self, v: impl Into<FlagSet<Metakey>>) -> Self {
        self.0 = self.0.map_args(|args| args.with_metakey(v));
        self
    }

    /// Call the function to consume all the input and generate a
    /// result.
    pub fn call(self) -> Result<BlockingLister> {
        self.0.call()
    }
}

/// Function that generated by [`BlockingOperator::read_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FunctionRead(pub(crate) OperatorFunction<OpRead, Vec<u8>>);

impl FunctionRead {
    /// Set the range for this operation.
    pub fn range(mut self, range: impl RangeBounds<u64>) -> Self {
        self.0 = self.0.map_args(|args| args.with_range(range.into()));
        self
    }

    /// Call the function to consume all the input and generate a
    /// result.
    pub fn call(self) -> Result<Vec<u8>> {
        self.0.call()
    }
}

/// Function that generated by [`BlockingOperator::reader_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FunctionReader(pub(crate) OperatorFunction<OpRead, BlockingReader>);

impl FunctionReader {
    /// Set the range for this operation.
    pub fn range(mut self, range: impl RangeBounds<u64>) -> Self {
        self.0 = self.0.map_args(|args| args.with_range(range.into()));
        self
    }

    /// Call the function to consume all the input and generate a
    /// result.
    pub fn call(self) -> Result<BlockingReader> {
        self.0.call()
    }
}

/// Function that generated by [`BlockingOperator::stat_with`].
///
/// Users can add more options by public functions provided by this struct.
pub struct FunctionStat(pub(crate) OperatorFunction<OpStat, Metadata>);

impl FunctionStat {
    /// Set the If-Match for this operation.
    pub fn if_match(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_if_match(v));
        self
    }

    /// Set the If-None-Match for this operation.
    pub fn if_none_match(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_if_none_match(v));
        self
    }

    /// Set the version for this operation.
    pub fn version(mut self, v: &str) -> Self {
        self.0 = self.0.map_args(|args| args.with_version(v));
        self
    }

    /// Call the function to consume all the input and generate a
    /// result.
    pub fn call(self) -> Result<Metadata> {
        self.0.call()
    }
}