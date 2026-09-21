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

use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use asyncband::event::ManualResetEvent;
use asyncband::mutex::Mutex as AsyncMutex;
use futures::future::select;
use opendal_core::{ErrorKind, Result};

use crate::AimdConfig;

/// One feedback window and token bucket share a lock so rate changes cannot
/// race with token refill or overwrite a newer rate.
#[derive(Debug)]
pub(crate) struct Bucket {
    config: AimdConfig,
    state: Mutex<State>,
    // The FIFO lock owns the waiting queue. Cancelling an acquire drops
    // its queue position without reserving tokens or leaving token debt.
    admission: AsyncMutex<()>,
    changed: ManualResetEvent,
}

#[derive(Debug)]
struct State {
    rate: f64,
    tokens: f64,
    last_refill: Instant,
    window_start: Instant,
    active: bool,
    throttled: bool,
}

impl State {
    fn advance(&mut self, config: &AimdConfig, now: Instant) {
        // Settle elapsed time at the old rate before adjusting it.
        self.tokens = (self.tokens
            + now.duration_since(self.last_refill).as_secs_f64() * self.rate)
            .min(f64::from(config.burst));
        self.last_refill = now;

        if now.duration_since(self.window_start) >= config.window {
            if self.throttled {
                self.rate = (self.rate * config.decrease_factor).max(config.min_rate);
            } else if self.active {
                self.rate = (self.rate + config.additive_increment).min(config.max_rate);
            }
            // Do not replay empty windows after an idle period.
            self.window_start = now;
            self.active = false;
            self.throttled = false;
        }
    }
}

impl Bucket {
    pub(crate) fn new(config: AimdConfig) -> Self {
        let now = Instant::now();
        Self {
            state: Mutex::new(State {
                rate: config.initial_rate,
                tokens: f64::from(config.burst),
                last_refill: now,
                window_start: now,
                active: false,
                throttled: false,
            }),
            config,
            admission: AsyncMutex::new(()),
            changed: ManualResetEvent::new(),
        }
    }

    pub(crate) async fn acquire<F, Fut>(&self, sleep: F)
    where
        F: Fn(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        let _turn = self.admission.lock().await;
        loop {
            // Only the queue head waits on this event. Reset before reading
            // state so feedback between the read and the wait remains signalled.
            self.changed.reset();
            let wait = {
                let mut state = self.state.lock().expect("AIMD state poisoned");
                let now = Instant::now();
                state.advance(&self.config, now);
                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    return;
                }
                let mut wait = Duration::from_secs_f64((1.0 - state.tokens) / state.rate)
                    .max(Duration::from_nanos(1));
                if state.active || state.throttled {
                    // Revisit a pending feedback window even if the next token
                    // would otherwise arrive later than the window boundary.
                    wait = wait.min(self.config.window - now.duration_since(state.window_start));
                }
                wait
            };
            let changed = std::pin::pin!(self.changed.wait());
            let timer = std::pin::pin!(sleep(wait));
            select(timer, changed).await;
        }
    }

    pub(crate) fn complete_http(&self) {
        let mut state = self.state.lock().expect("AIMD state poisoned");
        state.advance(&self.config, Instant::now());
        state.active = true;
        drop(state);
        self.changed.set();
    }

    pub(crate) fn observe<T>(&self, result: &Result<T>) {
        if result
            .as_ref()
            .is_err_and(|err| err.kind() == ErrorKind::RateLimited)
        {
            let mut state = self.state.lock().expect("AIMD state poisoned");
            state.advance(&self.config, Instant::now());
            state.throttled = true;
            drop(state);
            self.changed.set();
        }
    }
}

#[cfg(test)]
impl Bucket {
    pub(crate) fn rate(&self) -> f64 {
        self.state.lock().unwrap().rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{executor::block_on, poll};
    use opendal_core::Error;

    fn config() -> AimdConfig {
        AimdConfig {
            initial_rate: 4.0,
            min_rate: 1.0,
            max_rate: 8.0,
            additive_increment: 2.0,
            window: Duration::from_secs(1),
            burst: 1,
            ..Default::default()
        }
    }

    #[test]
    fn active_windows_grow_empty_windows_do_not_and_bounds_hold() {
        let config = config();
        let mut state = Bucket::new(config.clone()).state.into_inner().unwrap();
        let mut now = state.window_start;
        for expected in [6.0, 8.0, 8.0] {
            state.active = true;
            now += config.window;
            state.advance(&config, now);
            assert_eq!(state.rate, expected);
        }
        for expected in [4.0, 2.0, 1.0, 1.0] {
            state.active = true;
            state.throttled = true;
            now += config.window;
            state.advance(&config, now);
            assert_eq!(state.rate, expected);
            state.advance(&config, now);
            assert_eq!(state.rate, expected, "a window must be applied only once");
        }
        now += Duration::from_secs(100);
        state.advance(&config, now);
        assert_eq!(state.rate, 1.0);
        state.active = true;
        now += Duration::from_secs(100);
        state.advance(&config, now);
        assert_eq!(state.rate, 3.0, "idle windows must not be replayed");
    }

    #[test]
    fn only_rate_limited_errors_trigger_decrease() {
        let bucket = Bucket::new(config());
        bucket.observe::<()>(&Err(
            Error::new(ErrorKind::Unexpected, "retryable").set_temporary()
        ));
        bucket.observe::<()>(&Err(Error::new(ErrorKind::NotFound, "missing")));
        assert!(!bucket.state.lock().unwrap().throttled);
        assert!(!bucket.state.lock().unwrap().active);
        bucket.observe::<()>(&Err(Error::new(ErrorKind::RateLimited, "limited")));
        assert!(bucket.state.lock().unwrap().throttled);
    }

    #[test]
    fn refill_uses_old_rate_before_applying_window_feedback() {
        let config = AimdConfig {
            window: Duration::from_millis(100),
            ..config()
        };
        for (throttled, expected_rate, remaining) in [
            (true, 2.0, Duration::from_millis(300)),
            (false, 6.0, Duration::from_millis(100)),
        ] {
            let mut state = Bucket::new(config.clone()).state.into_inner().unwrap();
            state.tokens = 0.0;
            state.active = true;
            state.throttled = throttled;
            let now = state.window_start + config.window;
            state.advance(&config, now);
            assert_eq!(state.rate, expected_rate);
            assert!((state.tokens - 0.4).abs() < 1e-9);
            state.advance(&config, now + remaining);
            assert!((state.tokens - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn fifo_admission_and_cancellation_leave_no_token_debt() {
        block_on(async {
            let bucket = Bucket::new(AimdConfig {
                initial_rate: 0.001,
                min_rate: 0.001,
                ..config()
            });
            bucket.acquire(|_| std::future::pending::<()>()).await;
            let mut head = Box::pin(bucket.acquire(|_| std::future::pending::<()>()));
            let mut cancelled = Box::pin(bucket.acquire(|_| std::future::pending::<()>()));
            let mut tail = Box::pin(bucket.acquire(|_| std::future::pending::<()>()));
            assert!(poll!(&mut head).is_pending());
            assert!(poll!(&mut cancelled).is_pending());
            assert!(poll!(&mut tail).is_pending());
            drop(cancelled);
            bucket.state.lock().unwrap().tokens = 1.0;
            bucket.changed.set();
            assert!(poll!(&mut tail).is_pending(), "the queue head goes first");
            assert!(poll!(&mut head).is_ready());
            assert!(
                poll!(&mut tail).is_pending(),
                "only one token was available"
            );
            drop(tail);

            let mut cancelled_head = Box::pin(bucket.acquire(|_| std::future::pending::<()>()));
            let mut next = Box::pin(bucket.acquire(|_| std::future::pending::<()>()));
            assert!(poll!(&mut cancelled_head).is_pending());
            assert!(poll!(&mut next).is_pending());
            drop(cancelled_head);
            bucket.state.lock().unwrap().tokens = 1.0;
            bucket.changed.set();
            assert!(poll!(&mut next).is_ready());
        });
    }
}
