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
use cxx_async::CxxAsyncException;
use opendal as od;
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

#[cxx::bridge(namespace = "opendal::ffi::async")]
mod ffi {
    struct HashMapValue {
        key: String,
        value: String,
    }

    // Changed to use id-based approach instead of raw pointers
    // This eliminates the need for many unsafe blocks
    pub struct OperatorPtr {
        id: usize,
    }

    pub struct ReaderPtr {
        id: usize,
    }

    pub struct ListerPtr {
        id: usize,
    }

    extern "Rust" {

        fn new_operator(scheme: &str, configs: Vec<HashMapValue>) -> Result<usize>;
        fn operator_read(op: OperatorPtr, path: String) -> RustFutureRead;
        fn operator_write(op: OperatorPtr, path: String, bs: Vec<u8>) -> RustFutureWrite;
        fn operator_list(op: OperatorPtr, path: String) -> RustFutureList;
        fn operator_exists(op: OperatorPtr, path: String) -> RustFutureBool;
        fn operator_create_dir(op: OperatorPtr, path: String) -> RustFutureWrite;
        fn operator_copy(op: OperatorPtr, from: String, to: String) -> RustFutureWrite;
        fn operator_rename(op: OperatorPtr, from: String, to: String) -> RustFutureWrite;
        fn operator_delete(op: OperatorPtr, path: String) -> RustFutureWrite;
        fn operator_remove_all(op: OperatorPtr, path: String) -> RustFutureWrite;
        fn operator_reader(op: OperatorPtr, path: String) -> RustFutureReaderId;
        fn operator_lister(op: OperatorPtr, path: String) -> RustFutureListerId;

        fn reader_read(reader: ReaderPtr, start: u64, len: u64) -> RustFutureRead;
        fn lister_next(lister: ListerPtr) -> RustFutureEntryOption;

        fn delete_operator(op: OperatorPtr);
        fn delete_reader(reader: ReaderPtr);
        fn delete_lister(lister: ListerPtr);
    }

    extern "C++" {
        type RustFutureRead = super::RustFutureRead;
        type RustFutureWrite = super::RustFutureWrite;
        type RustFutureList = super::RustFutureList;
        type RustFutureBool = super::RustFutureBool;
        type RustFutureReaderId = super::RustFutureReaderId;
        type RustFutureListerId = super::RustFutureListerId;
        type RustFutureEntryOption = super::RustFutureEntryOption;
    }
}

#[cxx_async::bridge(namespace = opendal::ffi::async)]
unsafe impl Future for RustFutureRead {
    type Output = Vec<u8>;
}

#[cxx_async::bridge(namespace = opendal::ffi::async)]
unsafe impl Future for RustFutureWrite {
    type Output = ();
}

#[cxx_async::bridge(namespace = opendal::ffi::async)]
unsafe impl Future for RustFutureList {
    type Output = Vec<String>;
}

#[cxx_async::bridge(namespace = opendal::ffi::async)]
unsafe impl Future for RustFutureBool {
    type Output = bool;
}

#[cxx_async::bridge(namespace = opendal::ffi::async)]
unsafe impl Future for RustFutureReaderId {
    type Output = usize;
}

#[cxx_async::bridge(namespace = opendal::ffi::async)]
unsafe impl Future for RustFutureListerId {
    type Output = usize;
}

#[cxx_async::bridge(namespace = opendal::ffi::async)]
unsafe impl Future for RustFutureEntryOption {
    type Output = String;
}

// Global storage for operators, readers and listers to avoid Send issues with raw pointers
static OPERATOR_STORAGE: OnceLock<Mutex<HashMap<usize, Arc<od::Operator>>>> = OnceLock::new();
static OPERATOR_COUNTER: OnceLock<Mutex<usize>> = OnceLock::new();
static READER_STORAGE: OnceLock<Mutex<HashMap<usize, Arc<od::Reader>>>> = OnceLock::new();
static READER_COUNTER: OnceLock<Mutex<usize>> = OnceLock::new();
static LISTER_STORAGE: OnceLock<Mutex<HashMap<usize, Arc<Mutex<od::Lister>>>>> = OnceLock::new();
static LISTER_COUNTER: OnceLock<Mutex<usize>> = OnceLock::new();

fn get_operator_storage() -> &'static Mutex<HashMap<usize, Arc<od::Operator>>> {
    OPERATOR_STORAGE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_operator_counter() -> &'static Mutex<usize> {
    OPERATOR_COUNTER.get_or_init(|| Mutex::new(0))
}

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

fn new_operator(scheme: &str, configs: Vec<ffi::HashMapValue>) -> Result<usize> {
    let scheme = od::Scheme::from_str(scheme)?;

    let map: HashMap<String, String> = configs
        .into_iter()
        .map(|value| (value.key, value.value))
        .collect();

    let op = od::Operator::via_iter(scheme, map)?;

    // Store the operator in global storage and return an ID
    let op_arc = Arc::new(op);
    let counter = get_operator_counter();
    let mut counter_guard = counter
        .try_lock()
        .map_err(|_| anyhow::anyhow!("Failed to get operator counter lock"))?;
    *counter_guard += 1;
    let id = *counter_guard;
    drop(counter_guard);

    let storage = get_operator_storage();
    let mut storage_guard = storage
        .try_lock()
        .map_err(|_| anyhow::anyhow!("Failed to get operator storage lock"))?;
    storage_guard.insert(id, op_arc);
    drop(storage_guard);

    Ok(id)
}

fn operator_read(op: ffi::OperatorPtr, path: String) -> RustFutureRead {
    RustFutureRead::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        let result = operator
            .read(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;
        Ok(result.to_vec())
    })
}

fn operator_write(op: ffi::OperatorPtr, path: String, bs: Vec<u8>) -> RustFutureWrite {
    RustFutureWrite::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        operator
            .write(&path, bs)
            .await
            .map(|_| ())
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_list(op: ffi::OperatorPtr, path: String) -> RustFutureList {
    RustFutureList::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        let entries = operator
            .list(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;
        Ok(entries.into_iter().map(|e| e.path().to_string()).collect())
    })
}

fn operator_exists(op: ffi::OperatorPtr, path: String) -> RustFutureBool {
    RustFutureBool::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        operator
            .exists(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_create_dir(op: ffi::OperatorPtr, path: String) -> RustFutureWrite {
    RustFutureWrite::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        operator
            .create_dir(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_copy(op: ffi::OperatorPtr, from: String, to: String) -> RustFutureWrite {
    RustFutureWrite::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        operator
            .copy(&from, &to)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_rename(op: ffi::OperatorPtr, from: String, to: String) -> RustFutureWrite {
    RustFutureWrite::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        operator
            .rename(&from, &to)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_delete(op: ffi::OperatorPtr, path: String) -> RustFutureWrite {
    RustFutureWrite::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        operator
            .delete(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_remove_all(op: ffi::OperatorPtr, path: String) -> RustFutureWrite {
    RustFutureWrite::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        operator
            .remove_all(&path)
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))
    })
}

fn operator_reader(op: ffi::OperatorPtr, path: String) -> RustFutureReaderId {
    RustFutureReaderId::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        let reader = operator
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

fn operator_lister(op: ffi::OperatorPtr, path: String) -> RustFutureListerId {
    RustFutureListerId::fallible(async move {
        let storage = get_operator_storage().lock().await;
        let operator = storage
            .get(&op.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid operator ID".into()))?
            .clone();
        drop(storage);

        let lister = operator
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

fn reader_read(reader: ffi::ReaderPtr, start: u64, len: u64) -> RustFutureRead {
    RustFutureRead::fallible(async move {
        let storage = get_reader_storage().lock().await;
        let reader_arc = storage
            .get(&reader.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid reader ID".into()))?
            .clone();
        drop(storage);

        let buffer = reader_arc
            .read(start..(start + len))
            .await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;
        Ok(buffer.to_vec())
    })
}

fn lister_next(lister: ffi::ListerPtr) -> RustFutureEntryOption {
    RustFutureEntryOption::fallible(async move {
        use futures::TryStreamExt;

        let storage = get_lister_storage().lock().await;
        let lister_arc = storage
            .get(&lister.id)
            .ok_or_else(|| CxxAsyncException::new("Invalid lister ID".into()))?
            .clone();
        drop(storage);

        let mut lister_guard = lister_arc.lock().await;
        match lister_guard.try_next().await {
            Ok(Some(entry)) => Ok(entry.path().to_string()),
            Ok(None) => Ok(String::new()), // Empty string indicates end of iteration
            Err(e) => Err(CxxAsyncException::new(e.to_string().into_boxed_str())),
        }
    })
}

fn delete_operator(op: ffi::OperatorPtr) {
    // Use blocking lock since this is called from C++ destructors
    if let Ok(mut storage) = get_operator_storage().try_lock() {
        storage.remove(&op.id);
    }
    // If we can't get the lock immediately, we'll just skip cleanup
    // This is better than panicking in a destructor
}

fn delete_reader(reader: ffi::ReaderPtr) {
    // Use blocking lock since this is called from C++ destructors
    if let Ok(mut storage) = get_reader_storage().try_lock() {
        storage.remove(&reader.id);
    }
    // If we can't get the lock immediately, we'll just skip cleanup
    // This is better than panicking in a destructor
}

fn delete_lister(lister: ffi::ListerPtr) {
    // Use blocking lock since this is called from C++ destructors
    if let Ok(mut storage) = get_lister_storage().try_lock() {
        storage.remove(&lister.id);
    }
    // If we can't get the lock immediately, we'll just skip cleanup
    // This is better than panicking in a destructor
}
