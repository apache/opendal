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

use std::ffi::{c_char, CString};
use std::sync::{Mutex, OnceLock};
use tracing::subscriber::set_global_default;
use tracing::{Event, Level, Metadata, Subscriber};

// Define the C callback function pointer type.
// Parameters:
// - level: An integer representing the log level (e.g., 0 for INFO, 1 for WARN, 2 for ERROR, etc.)
// - file: The source file where the log originated (or NULL).
// - line: The line number in the source file (or 0).
// - message: The log message.
pub type opendal_glog_callback_t = Option<
    unsafe extern "C" fn(level: i32, file: *const c_char, line: u32, message: *const c_char),
>;

// Global static to store the C callback function pointer.
static GLOG_CALLBACK: OnceLock<Mutex<opendal_glog_callback_t>> = OnceLock::new();

fn get_glog_callback() -> &'static Mutex<opendal_glog_callback_t> {
    GLOG_CALLBACK.get_or_init(|| Mutex::new(None))
}

/// Initializes OpenDAL logging to forward logs to a C callback,
/// presumably for integration with glog or a similar C/C++ logging library.
///
/// This function should be called once at the beginning of the program.
///
/// # Safety
/// The `callback` function pointer must be valid for the lifetime of the program
/// if it is not NULL. It must also be safe to call from multiple threads if OpenDAL
/// operations are performed in a multi-threaded environment.
#[no_mangle]
pub unsafe extern "C" fn opendal_init_glog_logging(callback: opendal_glog_callback_t) {
    println!("[Rust] opendal_init_glog_logging called."); // DEBUG
    if callback.is_none() {
        eprintln!("opendal_init_glog_logging: received null callback, glog logging will not be initialized.");
        return;
    }

    // Store the callback
    let mut guard = get_glog_callback().lock().unwrap();
    *guard = callback;
    println!("[Rust] Glog callback stored."); // DEBUG

    // Create and set the custom glog subscriber
    let subscriber = GlogSubscriber {};
    match set_global_default(subscriber) {
        Ok(_) => println!("[Rust] GlogSubscriber set as global default."), // DEBUG
        Err(e) => eprintln!(
            "[Rust] opendal_init_glog_logging: failed to set global default subscriber: {}",
            e
        ),
    }
}

struct GlogSubscriber {}

impl GlogSubscriber {
    fn glog_level_from_tracing_level(level: &Level) -> i32 {
        match *level {
            Level::ERROR => 3, // FATAL/ERROR in glog (varies, using 3 for ERROR)
            Level::WARN => 2,  // WARNING in glog
            Level::INFO => 1,  // INFO in glog
            Level::DEBUG => 0, // DEBUG in glog (often 0 or verbose levels)
            Level::TRACE => 0, // TRACE in glog (often 0 or verbose levels)
        }
    }
}

// Using tracing_subscriber::Layer for easier integration if needed later,
// but for a simple global subscriber, implementing `Subscriber` directly is also fine.
// For now, let's implement Subscriber directly for simplicity.

impl Subscriber for GlogSubscriber {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        // We rely on the C-side (glog) to do its own filtering if needed.
        // Alternatively, one could check if the callback is Some here.
        get_glog_callback().lock().unwrap().is_some()
        // true // Force enable for debugging
    }

    fn new_span(&self, _span: &tracing::span::Attributes) -> tracing::span::Id {
        tracing::span::Id::from_u64(0) // No-op for now, glog is mostly for flat logs
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record) {
        // No-op for now
    }

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {
        // No-op
    }

    fn event(&self, event: &Event) {
        println!(
            "[Rust GlogSubscriber::event] Entered. Level: {:?}, Target: {}",
            event.metadata().level(),
            event.metadata().target()
        ); // DEBUG
        let glog_cb_guard = get_glog_callback().lock().unwrap();
        if let Some(cb) = *glog_cb_guard {
            let metadata = event.metadata();
            let level = Self::glog_level_from_tracing_level(metadata.level());
            let file = metadata.file().and_then(|f| CString::new(f).ok());
            let line = metadata.line().unwrap_or(0);

            // Create a visitor to extract the message
            struct MessageVisitor<'a>(&'a mut Option<String>);
            impl<'a> tracing::field::Visit for MessageVisitor<'a> {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" && self.0.is_none() {
                        // Take the first "message" field
                        *self.0 = Some(format!("{:?}", value));
                    }
                }
            }

            let mut message_opt = None;
            let mut visitor = MessageVisitor(&mut message_opt);
            event.record(&mut visitor);

            if let Some(message_str) = message_opt {
                // DEBUG PRINT
                println!("[Rust GlogSubscriber] Event message_str: {}", message_str);
                if let Ok(message_cstring) = CString::new(message_str) {
                    // message_str is String, CString takes ownership
                    unsafe {
                        cb(
                            level,
                            file.as_ref().map_or(std::ptr::null(), |cs| cs.as_ptr()),
                            line,
                            message_cstring.as_ptr(),
                        );
                    }
                } else {
                    eprintln!("OpenDAL glog layer: log message contained null byte and could not be sent to C.");
                }
            } else {
                // If there's no field named "message", try to format the whole event.
                // This is a fallback and might not be ideal for structured glog.
                let fallback_message = format!("{:?}", event.fields());
                // DEBUG PRINT
                println!(
                    "[Rust GlogSubscriber] Fallback message_str: {}",
                    fallback_message
                );
                if let Ok(message_cstring) = CString::new(fallback_message) {
                    // fallback_message is String
                    unsafe {
                        cb(
                            level,
                            file.as_ref().map_or(std::ptr::null(), |cs| cs.as_ptr()),
                            line,
                            message_cstring.as_ptr(),
                        );
                    }
                } else {
                    eprintln!("OpenDAL glog layer: fallback log message contained null byte.");
                }
            }
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {
        // No-op
    }

    fn exit(&self, _span: &tracing::span::Id) {
        // No-op
    }
}
