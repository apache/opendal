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

use std::cell::Cell;
use std::collections::VecDeque;
use std::ffi::CString;
use std::ffi::c_char;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

pub type LogCallback = extern "C" fn(u32, *const c_char);

/// Cap buffered log records so memory stays bounded if logs are emitted while
/// no Haskell FFI call is actively draining them.
const MAX_BUFFERED_LOGS: usize = 4096;

struct HsLogger {
    initialized: Mutex<bool>,
    callback: RwLock<Option<LogCallback>>,
    flush_lock: Mutex<()>,
    pending: Mutex<VecDeque<(u32, CString)>>,
}

static HS_LOGGER: HsLogger = HsLogger {
    initialized: Mutex::new(false),
    callback: RwLock::new(None),
    flush_lock: Mutex::new(()),
    pending: Mutex::new(VecDeque::new()),
};

static LOGGER_ENABLED: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// Prevent re-entrant log flushing if a Haskell log action triggers
    /// another OpenDAL FFI call on the same thread (which would otherwise
    /// risk recursive `RwLock::read` deadlock or infinite log recursion).
    static IN_LOG_FLUSH: Cell<bool> = const { Cell::new(false) };
}

struct InLogFlushGuard;

impl Drop for InLogFlushGuard {
    fn drop(&mut self) {
        IN_LOG_FLUSH.with(|flag| flag.set(false));
    }
}

/// RAII guard that flushes buffered log records on the calling Haskell OS
/// thread right before an FFI entrypoint returns.
pub(crate) struct FlushLogsGuard;

impl Drop for FlushLogsGuard {
    fn drop(&mut self) {
        flush_logs();
    }
}

/// Install or replace the global Haskell log callback.
///
/// Registers the single static `HsLogger` with the `log` crate on first use
/// and swaps the active callback under a write lock so any in-flight callback
/// invocations complete before the previous `FunPtr` is returned to Haskell
/// for `freeHaskellFunPtr`.
pub(crate) fn install_logger(
    callback: LogCallback,
) -> Result<Option<LogCallback>, log::SetLoggerError> {
    // Deliver any buffered records to the previous callback before swapping it.
    flush_logs();

    {
        let mut initialized = HS_LOGGER
            .initialized
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !*initialized {
            log::set_logger(&HS_LOGGER)?;
            log::set_max_level(log::LevelFilter::Debug);
            *initialized = true;
        }
    }

    let mut guard = HS_LOGGER
        .callback
        .write()
        .unwrap_or_else(|e| e.into_inner());
    let prev = guard.replace(callback);
    LOGGER_ENABLED.store(true, Ordering::Release);
    Ok(prev)
}

/// Drain buffered log records and invoke the registered Haskell callback on
/// the current (calling Haskell) OS thread.
///
/// Must only be called from a `safe` FFI entrypoint on the Haskell calling
/// thread — never from Tokio worker threads or `ForeignPtr` GC finalizers.
pub(crate) fn flush_logs() {
    if !LOGGER_ENABLED.load(Ordering::Acquire) {
        return;
    }

    let already_flushing = IN_LOG_FLUSH.with(|flag| {
        if flag.get() {
            true
        } else {
            flag.set(true);
            false
        }
    });
    if already_flushing {
        return;
    }
    let _reentrancy_guard = InLogFlushGuard;
    // Serialize flushing so buffered records are delivered in FIFO order and
    // any records popped by a concurrent thread finish invoking the callback
    // before this call returns.
    let _flush_guard = HS_LOGGER
        .flush_lock
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    let batch: Vec<(u32, CString)> = {
        let mut pending = HS_LOGGER.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.drain(..).collect()
    };
    if batch.is_empty() {
        return;
    }

    let guard = HS_LOGGER.callback.read().unwrap_or_else(|e| e.into_inner());
    let Some(callback) = *guard else {
        return;
    };
    for (level, msg) in batch {
        callback(level, msg.as_ptr());
    }
}

impl log::Log for HsLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Debug
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let msg = format!("{}", record.args());
        // Interior NUL bytes would make CString::new fail and panic across
        // the FFI boundary; escape them instead of aborting the process.
        let c_str_msg = CString::new(msg.replace('\0', "\\0")).unwrap_or_default();
        let c_level = match record.level() {
            log::Level::Debug => 0,
            log::Level::Info => 1,
            log::Level::Warn => 2,
            log::Level::Error => 3,
            log::Level::Trace => 0,
        };

        // Never invoke the Haskell FunPtr directly from arbitrary Tokio worker
        // threads or GC finalizers; enqueue and flush on the calling Haskell
        // OS thread before the FFI call returns.
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.len() >= MAX_BUFFERED_LOGS {
            pending.pop_front();
        }
        pending.push_back((c_level, c_str_msg));
    }

    fn flush(&self) {}
}
