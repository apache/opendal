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
use std::ffi::{CString, c_char};
use std::sync::{Arc, Mutex};

pub type LogCallback = extern "C" fn(u32, *const c_char);
pub type FreeLogCallback = extern "C" fn(ErasedLogCallback);
pub type ErasedLogCallback = unsafe extern "C" fn();

const MAX_BUFFERED_LOGS: usize = 4096;

struct Callback {
    call: LogCallback,
    free: FreeLogCallback,
}

impl Drop for Callback {
    fn drop(&mut self) {
        // SAFETY: GHC's hs_free_fun_ptr takes an erased function pointer.
        // It releases the wrapper without calling it through this type.
        let erased = unsafe { std::mem::transmute::<LogCallback, ErasedLogCallback>(self.call) };
        (self.free)(erased);
    }
}

struct LoggerState {
    callback: Option<Arc<Callback>>,
    // The runtime can emit logs on threads that cannot call into a non-threaded GHC RTS.
    pending: VecDeque<(u32, CString)>,
}

static HS_LOGGER: HsLogger = HsLogger(Mutex::new(LoggerState {
    callback: None,
    pending: VecDeque::new(),
}));

struct HsLogger(Mutex<LoggerState>);

thread_local! {
    static IN_LOG_FLUSH: Cell<bool> = const { Cell::new(false) };
}

struct InLogFlushGuard;

impl Drop for InLogFlushGuard {
    fn drop(&mut self) {
        IN_LOG_FLUSH.set(false);
    }
}

/// Flush on return from a Haskell operation, after the runtime has finished polling it.
pub(crate) struct FlushLogsGuard;

impl Drop for FlushLogsGuard {
    fn drop(&mut self) {
        flush_logs();
    }
}

/// Take ownership of the callback on success. A drain in progress keeps its
/// callback alive until it finishes, even if the action installs a new logger.
pub(crate) fn install_logger(
    call: LogCallback,
    free: FreeLogCallback,
) -> Result<(), log::SetLoggerError> {
    flush_logs();
    let previous = {
        let mut state = HS_LOGGER.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.callback.is_none() {
            log::set_logger(&HS_LOGGER)?;
            log::set_max_level(log::LevelFilter::Debug);
        }
        state.callback.replace(Arc::new(Callback { call, free }))
    };
    drop(previous);
    Ok(())
}

/// Run callbacks only from safe FFI calls, with no logger locks held.
fn flush_logs() {
    if IN_LOG_FLUSH.replace(true) {
        return;
    }
    let _guard = InLogFlushGuard;

    let (callback, batch) = {
        let mut state = HS_LOGGER.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.pending.is_empty() {
            return;
        }
        (state.callback.clone(), std::mem::take(&mut state.pending))
    };
    if let Some(callback) = callback {
        for (level, msg) in batch {
            (callback.call)(level, msg.as_ptr());
        }
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

        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.pending.len() >= MAX_BUFFERED_LOGS {
            state.pending.pop_front();
        }
        state.pending.push_back((c_level, c_str_msg));
    }

    fn flush(&self) {}
}
