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
use asyncband::mutex::Mutex;
use cxx_async::CxxAsyncException;
use opendal as od;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

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

pub struct Operator(od::Operator);

// Global storage for readers and listers to avoid Send issues with raw pointers
static READER_STORAGE: OnceLock<Mutex<HashMap<usize, Arc<od::Reader>>>> = OnceLock::new();
static READER_COUNTER: OnceLock<Mutex<usize>> = OnceLock::new();
static LISTER_STORAGE: OnceLock<Mutex<HashMap<usize, Arc<Mutex<od::Lister>>>>> = OnceLock::new();
static LISTER_COUNTER: OnceLock<Mutex<usize>> = OnceLock::new();

fn get_reader_storage() -> &'static Mutex<HashMap<usize, Arc<od::Reader>>> {
    READER_STORAGE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_reader_counter() -> &'static Mutex<usize> {
    READER_COUNTER.get_or_init(|| Mutex::new(0))
}

fn get_lister_storage() -> &'static Mutex<HashMap<usize, Arc<Mutex<od::Lister>>>> {
    LISTER_STORAGE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_lister_counter() -> &'static Mutex<usize> {
    LISTER_COUNTER.get_or_init(|| Mutex::new(0))
}

fn new_operator(scheme: &str, configs: Vec<ffi::HashMapValue>) -> Result<Box<Operator>> {
    let map: HashMap<String, String> = configs
        .into_iter()
        .map(|value| (value.key, value.value))
        .collect();

    od::install_default();

    let op = Box::new(Operator(od::Operator::via_iter(scheme, map)?));

    Ok(op)
}

// The returned futures are polled lazily and must stay 'static: each one
// captures an owned `od::Operator` clone (an `Arc`-backed handle, cheap to
// clone) instead of borrowing `op`, so the future remains valid even if the
// C++ operator is destroyed before the future is first polled.
fn operator_read(op: &Operator, path: String) -> RustFutureRead {
    let op = op.0.clone();
    RustFutureRead::fallible(async move {
        Ok(op
            .read(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?
            .to_vec())
    })
}

fn operator_write(op: &Operator, path: String, bs: &[u8]) -> RustFutureWrite {
    let op = op.0.clone();
    // Copy before building the future, which outlives the borrowed slice.
    let bs = bs.to_vec();
    RustFutureWrite::fallible(async move {
        op.write(&path, bs)
            .await
            .map(|_| ())
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_list(op: &Operator, path: String) -> RustFutureList {
    let op = op.0.clone();
    RustFutureList::fallible(async move {
        let entries = op
            .list(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;
        Ok(entries.into_iter().map(|e| e.path().to_string()).collect())
    })
}

fn operator_exists(op: &Operator, path: String) -> RustFutureBool {
    let op = op.0.clone();
    RustFutureBool::fallible(async move {
        op.exists(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_create_dir(op: &Operator, path: String) -> RustFutureWrite {
    let op = op.0.clone();
    RustFutureWrite::fallible(async move {
        op.create_dir(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_copy(op: &Operator, from: String, to: String) -> RustFutureWrite {
    let op = op.0.clone();
    RustFutureWrite::fallible(async move {
        op.copy(&from, &to)
            .await
            .map(|_| ())
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_rename(op: &Operator, from: String, to: String) -> RustFutureWrite {
    let op = op.0.clone();
    RustFutureWrite::fallible(async move {
        op.rename(&from, &to)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_delete(op: &Operator, path: String) -> RustFutureWrite {
    let op = op.0.clone();
    RustFutureWrite::fallible(async move {
        op.delete(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_reader(op: &Operator, path: String) -> RustFutureReaderId {
    let op = op.0.clone();
    RustFutureReaderId::fallible(async move {
        let reader = op
            .reader(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;

        // Store the reader in global storage and return an ID
        let reader_arc = Arc::new(reader);
        let mut counter = get_reader_counter().lock().await;
        *counter += 1;
        let id = *counter;

        let mut storage = get_reader_storage().lock().await;
        storage.insert(id, reader_arc);

        Ok(id)
    })
}

fn operator_lister(op: &Operator, path: String) -> RustFutureListerId {
    let op = op.0.clone();
    RustFutureListerId::fallible(async move {
        let lister = op
            .lister(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;

        // Store the lister in global storage and return an ID
        let lister_arc = Arc::new(Mutex::new(lister));
        let mut counter = get_lister_counter().lock().await;
        *counter += 1;
        let id = *counter;

        let mut storage = get_lister_storage().lock().await;
        storage.insert(id, lister_arc);

        Ok(id)
    })
}

fn reader_read(reader: usize, start: u64, len: u64) -> RustFutureRead {
    // The C++ `Reader` only passes its id. Resolve it eagerly so the returned
    // future owns the reader handle and stays valid even if the C++ reader is
    // destroyed before the future is polled. The global storage uses an async
    // mutex, so fall back to resolving at first poll when it is contended.
    let resolved_reader = get_reader_storage()
        .try_lock()
        .and_then(|storage| storage.get(&reader).cloned());
    RustFutureRead::fallible(async move {
        let reader_arc = match resolved_reader {
            Some(reader_arc) => reader_arc,
            None => {
                let storage = get_reader_storage().lock().await;
                storage
                    .get(&reader)
                    .ok_or_else(|| CxxAsyncException::new("Invalid reader ID".into()))?
                    .clone()
            }
        };

        let buffer = reader_arc
            .read(start..(start + len))
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;
        Ok(buffer.to_vec())
    })
}

fn lister_next(lister: usize) -> RustFutureEntryOption {
    // Same ownership rule as `reader_read`: resolve the id eagerly so the
    // future owns the lister handle.
    let resolved_lister = get_lister_storage()
        .try_lock()
        .and_then(|storage| storage.get(&lister).cloned());
    RustFutureEntryOption::fallible(async move {
        use futures::TryStreamExt;

        let lister_arc = match resolved_lister {
            Some(lister_arc) => lister_arc,
            None => {
                let storage = get_lister_storage().lock().await;
                storage
                    .get(&lister)
                    .ok_or_else(|| CxxAsyncException::new("Invalid lister ID".into()))?
                    .clone()
            }
        };

        let mut lister_guard = lister_arc.lock().await;
        match lister_guard.try_next().await {
            Ok(Some(entry)) => Ok(entry.path().to_string()),
            Ok(None) => Ok(String::new()), // Empty string indicates end of iteration
            Err(e) => Err(CxxAsyncException::new(e.to_string().into_boxed_str())),
        }
    })
}

fn delete_reader(reader: usize) {
    // Use blocking lock since this is called from C++ destructors
    if let Some(mut storage) = get_reader_storage().try_lock() {
        storage.remove(&reader);
    }
    // If we can't get the lock immediately, we'll just skip cleanup
    // This is better than panicking in a destructor
}

fn delete_lister(lister: usize) {
    // Use blocking lock since this is called from C++ destructors
    if let Some(mut storage) = get_lister_storage().try_lock() {
        storage.remove(&lister);
    }
    // If we can't get the lock immediately, we'll just skip cleanup
    // This is better than panicking in a destructor
}
