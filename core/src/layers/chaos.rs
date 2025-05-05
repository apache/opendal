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

use std::sync::Arc;
use std::sync::Mutex;

use rand::prelude::*;
use rand::rngs::StdRng;

use crate::raw::*;
use crate::*;

/// Inject chaos into underlying services for robustness test.
///
/// # Chaos
///
/// Chaos tests is a part of stress test. By generating errors at specified
/// error ratio, we can reproduce underlying services error more reliable.
///
/// Running tests under ChaosLayer will make your application more robust.
///
/// For example: If we specify an error rate of 0.5, there is a 50% chance
/// of an EOF error for every read operation.
///
/// # Note
///
/// For now, ChaosLayer only injects read operations. More operations may
/// be added in the future.
///
/// # Examples
///
/// ```no_run
/// # use opendal::layers::ChaosLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(ChaosLayer::new(0.1))
///     .finish();
/// Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ChaosLayer {
    error_ratio: f64,
}

impl ChaosLayer {
    /// Create a new chaos layer with specified error ratio.
    ///
    /// # Panics
    ///
    /// Input error_ratio must in [0.0..=1.0]
    pub fn new(error_ratio: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&error_ratio),
            "error_ratio must between 0.0 and 1.0"
        );
        Self { error_ratio }
    }
}

impl<A: Access> Layer<A> for ChaosLayer {
    type LayeredAccess = ChaosAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        ChaosAccessor {
            inner,
            rng: StdRng::from_entropy(),
            error_ratio: self.error_ratio,
        }
    }
}

#[derive(Debug)]
pub struct ChaosAccessor<A> {
    inner: A,
    rng: StdRng,

    error_ratio: f64,
}

impl<A: Access> LayeredAccess for ChaosAccessor<A> {
    type Inner = A;
    type Reader = ChaosReader<A::Reader>;
    type BlockingReader = ChaosReader<A::BlockingReader>;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;
    type Deleter = A::Deleter;
    type BlockingDeleter = A::BlockingDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ChaosReader::new(r, self.rng.clone(), self.error_ratio)))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, ChaosReader::new(r, self.rng.clone(), self.error_ratio)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args)
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        self.inner.blocking_delete()
    }
}

/// ChaosReader will inject error into read operations.
pub struct ChaosReader<R> {
    inner: R,
    rng: Arc<Mutex<StdRng>>,

    error_ratio: f64,
}

impl<R> ChaosReader<R> {
    fn new(inner: R, rng: StdRng, error_ratio: f64) -> Self {
        Self {
            inner,
            rng: Arc::new(Mutex::new(rng)),
            error_ratio,
        }
    }

    /// If I feel lucky, we can return the correct response. Otherwise,
    /// we need to generate an error.
    fn i_feel_lucky(&self) -> bool {
        let point = self.rng.lock().unwrap().gen_range(0..=100);
        point >= (self.error_ratio * 100.0) as i32
    }

    fn unexpected_eof() -> Error {
        Error::new(ErrorKind::Unexpected, "I am your chaos!")
            .with_operation("chaos")
            .set_temporary()
    }
}

impl<R: oio::Read> oio::Read for ChaosReader<R> {
    async fn read(&mut self) -> Result<Buffer> {
        if self.i_feel_lucky() {
            self.inner.read().await
        } else {
            Err(Self::unexpected_eof())
        }
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for ChaosReader<R> {
    fn read(&mut self) -> Result<Buffer> {
        if self.i_feel_lucky() {
            self.inner.read()
        } else {
            Err(Self::unexpected_eof())
        }
    }
}
