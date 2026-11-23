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

use anyhow::Result;
use asyncband::mutex::Mutex as AsyncMutex;
use cxx_async::CxxAsyncException;
use opendal as od;
use std::collections::HashMap;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::task::{Context, Poll};

#[cxx::bridge(namespace = opendal::ffi::async_op)]
mod ffi {
    struct HashMapValue {
        key: String,
        value: String,
    }

    extern "Rust" {
        type Operator;

        fn new_operator(scheme: &str, configs: Vec<HashMapValue>) -> Result<Box<Operator>>;
        fn operator_read(op: &Operator, path: String) -> RustFutureRead;
        fn operator_write(op: &Operator, path: String, bs: &[u8]) -> RustFutureWrite;
        fn operator_list(op: &Operator, path: String) -> RustFutureList;
        fn operator_exists(op: &Operator, path: String) -> RustFutureBool;
        fn operator_create_dir(op: &Operator, path: String) -> RustFutureWrite;
        fn operator_copy(op: &Operator, from: String, to: String) -> RustFutureWrite;
        fn operator_rename(op: &Operator, from: String, to: String) -> RustFutureWrite;
        fn operator_delete(op: &Operator, path: String) -> RustFutureWrite;
        fn operator_reader(op: &Operator, path: String) -> RustFutureReaderId;
        fn operator_lister(op: &Operator, path: String) -> RustFutureListerId;

        fn reader_read(reader: usize, start: u64, len: u64) -> RustFutureRead;
        fn lister_next(lister: usize) -> RustFutureEntryOption;

        fn delete_reader(reader: usize);
        fn delete_lister(lister: usize);
    }

    extern "C++" {
        #[namespace = opendal::ffi::async_op]
        type RustFutureRead = super::RustFutureRead;
        #[namespace = opendal::ffi::async_op]
        type RustFutureWrite = super::RustFutureWrite;
        #[namespace = opendal::ffi::async_op]
        type RustFutureList = super::RustFutureList;
        #[namespace = opendal::ffi::async_op]
        type RustFutureBool = super::RustFutureBool;
        #[namespace = opendal::ffi::async_op]
        type RustFutureReaderId = super::RustFutureReaderId;
        #[namespace = opendal::ffi::async_op]
        type RustFutureListerId = super::RustFutureListerId;
        #[namespace = opendal::ffi::async_op]
        type RustFutureEntryOption = super::RustFutureEntryOption;
    }
}

#[cxx_async::bridge(namespace = opendal::ffi::async_op)]
unsafe impl Future for RustFutureRead {
    type Output = Vec<u8>;
}

#[cxx_async::bridge(namespace = opendal::ffi::async_op)]
unsafe impl Future for RustFutureWrite {
    type Output = ();
}

#[cxx_async::bridge(namespace = opendal::ffi::async_op)]
unsafe impl Future for RustFutureList {
    type Output = Vec<String>;
}

#[cxx_async::bridge(namespace = opendal::ffi::async_op)]
unsafe impl Future for RustFutureBool {
    type Output = bool;
}

#[cxx_async::bridge(namespace = opendal::ffi::async_op)]
unsafe impl Future for RustFutureReaderId {
    type Output = usize;
}

#[cxx_async::bridge(namespace = opendal::ffi::async_op)]
unsafe impl Future for RustFutureListerId {
    type Output = usize;
}

#[cxx_async::bridge(namespace = opendal::ffi::async_op)]
unsafe impl Future for RustFutureEntryOption {
    type Output = String;
}

/// Wraps an async future so polling and dropping always run inside the binding's
/// Tokio runtime context. `cxx_async` polls and drops futures directly on the
/// C++ caller thread, where Tokio-backed services would otherwise panic with
/// "there is no reactor running" and abort the process.
struct TokioRuntimeFuture<F> {
    future: ManuallyDrop<F>,
}

impl<F> TokioRuntimeFuture<F> {
    fn new(future: F) -> Self {
        Self {
            future: ManuallyDrop::new(future),
        }
    }
}

impl<F: Future> Future for TokioRuntimeFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = crate::RUNTIME.enter();
        // SAFETY: `future` is structurally pinned to `self`: nothing moves out
        // of `self.future`, and `Drop` drops it in place before `self` is
        // deallocated.
        let future = unsafe { self.map_unchecked_mut(|this| &mut *this.future) };
        future.poll(cx)
    }
}

impl<F> Drop for TokioRuntimeFuture<F> {
    fn drop(&mut self) {
        let _guard = crate::RUNTIME.enter();
        // SAFETY: `future` is only dropped here, once, in place.
        unsafe {
            ManuallyDrop::drop(&mut self.future);
        }
    }
}

pub struct Operator(ManuallyDrop<od::Operator>);

impl Operator {
    fn new(op: od::Operator) -> Self {
        Self(ManuallyDrop::new(op))
    }

    fn clone_inner(&self) -> od::Operator {
        (*self.0).clone()
    }
}

impl Drop for Operator {
    fn drop(&mut self) {
        let _guard = crate::RUNTIME.enter();
        // SAFETY: `self.0` is only dropped here, once.
        unsafe {
            ManuallyDrop::drop(&mut self.0);
        }
    }
}

// Global storage for readers and listers to avoid Send issues with raw pointers.
// The outer tables are only locked briefly and never held across `.await`, so a
// std mutex is enough; the inner lister mutex stays async because `lister_next`
// holds it across `try_next().await`.
static READER_STORAGE: OnceLock<Mutex<HashMap<usize, Arc<od::Reader>>>> = OnceLock::new();
static READER_COUNTER: OnceLock<Mutex<usize>> = OnceLock::new();
static LISTER_STORAGE: OnceLock<Mutex<HashMap<usize, Arc<AsyncMutex<od::Lister>>>>> =
    OnceLock::new();
static LISTER_COUNTER: OnceLock<Mutex<usize>> = OnceLock::new();

fn get_reader_storage() -> &'static Mutex<HashMap<usize, Arc<od::Reader>>> {
    READER_STORAGE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_reader_counter() -> &'static Mutex<usize> {
    READER_COUNTER.get_or_init(|| Mutex::new(0))
}

fn get_lister_storage() -> &'static Mutex<HashMap<usize, Arc<AsyncMutex<od::Lister>>>> {
    LISTER_STORAGE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_lister_counter() -> &'static Mutex<usize> {
    LISTER_COUNTER.get_or_init(|| Mutex::new(0))
}

/// Locks a std mutex without propagating poisoning: a panic in a previous
/// holder must not abort a C++ destructor or an FFI call.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn new_operator(scheme: &str, configs: Vec<ffi::HashMapValue>) -> Result<Box<Operator>> {
    let map: HashMap<String, String> = configs
        .into_iter()
        .map(|value| (value.key, value.value))
        .collect();

    let _guard = crate::RUNTIME.enter();
    od::install_default();

    let op = Box::new(Operator::new(od::Operator::via_iter(scheme, map)?));

    Ok(op)
}

// The returned futures are polled lazily and must stay 'static: each one
// captures an owned `od::Operator` clone (an `Arc`-backed handle, cheap to
// clone) instead of borrowing `op`, so the future remains valid even if the
// C++ operator is destroyed before the future is first polled.
fn operator_read(op: &Operator, path: String) -> RustFutureRead {
    let op = op.clone_inner();
    RustFutureRead::fallible(TokioRuntimeFuture::new(async move {
        Ok(op
            .read(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?
            .to_vec())
    }))
}

fn operator_write(op: &Operator, path: String, bs: &[u8]) -> RustFutureWrite {
    let op = op.clone_inner();
    // Copy before building the future, which outlives the borrowed slice.
    let bs = bs.to_vec();
    RustFutureWrite::fallible(TokioRuntimeFuture::new(async move {
        op.write(&path, bs)
            .await
            .map(|_| ())
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    }))
}

fn operator_list(op: &Operator, path: String) -> RustFutureList {
    let op = op.clone_inner();
    RustFutureList::fallible(TokioRuntimeFuture::new(async move {
        let entries = op
            .list(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;
        Ok(entries.into_iter().map(|e| e.path().to_string()).collect())
    }))
}

fn operator_exists(op: &Operator, path: String) -> RustFutureBool {
    let op = op.clone_inner();
    RustFutureBool::fallible(TokioRuntimeFuture::new(async move {
        op.exists(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    }))
}

fn operator_create_dir(op: &Operator, path: String) -> RustFutureWrite {
    let op = op.clone_inner();
    RustFutureWrite::fallible(TokioRuntimeFuture::new(async move {
        op.create_dir(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    }))
}

fn operator_copy(op: &Operator, from: String, to: String) -> RustFutureWrite {
    let op = op.clone_inner();
    RustFutureWrite::fallible(TokioRuntimeFuture::new(async move {
        op.copy(&from, &to)
            .await
            .map(|_| ())
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    }))
}

fn operator_rename(op: &Operator, from: String, to: String) -> RustFutureWrite {
    let op = op.clone_inner();
    RustFutureWrite::fallible(TokioRuntimeFuture::new(async move {
        op.rename(&from, &to)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    }))
}

fn operator_delete(op: &Operator, path: String) -> RustFutureWrite {
    let op = op.clone_inner();
    RustFutureWrite::fallible(TokioRuntimeFuture::new(async move {
        op.delete(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    }))
}

fn operator_reader(op: &Operator, path: String) -> RustFutureReaderId {
    let op = op.clone_inner();
    RustFutureReaderId::fallible(TokioRuntimeFuture::new(async move {
        let reader = op
            .reader(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;

        // Store the reader in global storage and return an ID
        let reader_arc = Arc::new(reader);
        let id = {
            let mut counter = lock(get_reader_counter());
            *counter += 1;
            *counter
        };
        lock(get_reader_storage()).insert(id, reader_arc);

        Ok(id)
    }))
}

fn operator_lister(op: &Operator, path: String) -> RustFutureListerId {
    let op = op.clone_inner();
    RustFutureListerId::fallible(TokioRuntimeFuture::new(async move {
        let lister = op
            .lister(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;

        // Store the lister in global storage and return an ID
        let lister_arc = Arc::new(AsyncMutex::new(lister));
        let id = {
            let mut counter = lock(get_lister_counter());
            *counter += 1;
            *counter
        };
        lock(get_lister_storage()).insert(id, lister_arc);

        Ok(id)
    }))
}

fn reader_read(reader: usize, start: u64, len: u64) -> RustFutureRead {
    // The C++ `Reader` only passes its id. Resolve it eagerly so the returned
    // future owns the reader handle and stays valid even if the C++ reader is
    // destroyed before the future is polled.
    let resolved_reader = lock(get_reader_storage()).get(&reader).cloned();
    RustFutureRead::fallible(TokioRuntimeFuture::new(async move {
        let reader_arc =
            resolved_reader.ok_or_else(|| CxxAsyncException::new("Invalid reader ID".into()))?;

        let end = start
            .checked_add(len)
            .ok_or_else(|| CxxAsyncException::new("read range end overflow".into()))?;

        let buffer = reader_arc
            .read(start..end)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;
        Ok(buffer.to_vec())
    }))
}

fn lister_next(lister: usize) -> RustFutureEntryOption {
    // Same ownership rule as `reader_read`: resolve the id eagerly so the
    // future owns the lister handle.
    let resolved_lister = lock(get_lister_storage()).get(&lister).cloned();
    RustFutureEntryOption::fallible(TokioRuntimeFuture::new(async move {
        use futures::TryStreamExt;

        let lister_arc =
            resolved_lister.ok_or_else(|| CxxAsyncException::new("Invalid lister ID".into()))?;

        let mut lister_guard = lister_arc.lock().await;
        match lister_guard.try_next().await {
            Ok(Some(entry)) => Ok(entry.path().to_string()),
            Ok(None) => Ok(String::new()), // Empty string indicates end of iteration
            Err(e) => Err(CxxAsyncException::new(e.to_string().into_boxed_str())),
        }
    }))
}

fn delete_reader(reader: usize) {
    // Called from C++ destructors: always evict the id, then drop the removed
    // handle outside the lock within the Tokio runtime context.
    let removed = lock(get_reader_storage()).remove(&reader);
    let _guard = crate::RUNTIME.enter();
    drop(removed);
}

fn delete_lister(lister: usize) {
    // Called from C++ destructors: always evict the id, then drop the removed
    // handle outside the lock within the Tokio runtime context.
    let removed = lock(get_lister_storage()).remove(&lister);
    let _guard = crate::RUNTIME.enter();
    drop(removed);
}
