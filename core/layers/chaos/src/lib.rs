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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]
use std::sync::Arc;
use std::sync::Mutex;

use opendal_core::raw::*;
use opendal_core::*;
use rand::prelude::*;
use rand::rngs::StdRng;

/// Register the `chaos` layer with an operator registry.
///
/// Requires a finite `error_ratio` between `0` and `1`, inclusive. Each load
/// creates its own random number generator.
pub fn register_chaos_layer(registry: &OperatorRegistry) {
    registry.register_layer("chaos", |options| {
        let value = options
            .get("error_ratio")
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "error_ratio is required"))?;
        match value.parse::<f64>() {
            Ok(ratio) if (0.0..=1.0).contains(&ratio) => Ok(ChaosLayer::new(ratio)),
            _ => Err(Error::new(
                ErrorKind::ConfigInvalid,
                "error_ratio must be between 0 and 1 inclusive",
            )),
        }
    });
}

/// `ChaosLayer` injects errors into services to test robustness.
///
/// # Chaos
///
/// Chaos testing complements stress testing. A specified error ratio reproduces
/// service errors consistently.
///
/// Tests that use `ChaosLayer` can expose error-handling weaknesses.
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
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_chaos::ChaosLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(ChaosLayer::new(0.1));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ChaosLayer {
    rng: Arc<Mutex<StdRng>>,
    error_ratio: f64,
}

impl ChaosLayer {
    /// Create a new [`ChaosLayer`] with specified error ratio.
    ///
    /// # Panics
    ///
    /// Input error_ratio must in [0.0..=1.0]
    pub fn new(error_ratio: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&error_ratio),
            "error_ratio must between 0.0 and 1.0"
        );
        Self {
            rng: Arc::new(Mutex::new(StdRng::from_rng(&mut rand::rng()))),
            error_ratio,
        }
    }
}

impl Layer for ChaosLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(ChaosService {
            inner,
            rng: self.rng.clone(),
            error_ratio: self.error_ratio,
        })
    }
}

#[derive(Debug)]
struct ChaosService {
    inner: Servicer,
    rng: Arc<Mutex<StdRng>>,
    error_ratio: f64,
}

impl Service for ChaosService {
    type Reader = ChaosReader<oio::Reader>;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability();
        capability.write_can_copy_from = false;
        capability
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner
            .read(ctx, path, args)
            .map(|r| ChaosReader::new(r, Arc::clone(&self.rng), self.error_ratio))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.inner.restore(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

struct ChaosReader<R> {
    inner: R,
    rng: Arc<Mutex<StdRng>>,

    error_ratio: f64,
}

impl<R> ChaosReader<R> {
    fn new(inner: R, rng: Arc<Mutex<StdRng>>, error_ratio: f64) -> Self {
        Self {
            inner,
            rng,
            error_ratio,
        }
    }

    /// If I feel lucky, we can return the correct response. Otherwise,
    /// we need to generate an error.
    fn i_feel_lucky(&self) -> bool {
        let point: f64 = self.rng.lock().unwrap().random();
        point >= self.error_ratio
    }

    fn unexpected_eof() -> Error {
        Error::new(ErrorKind::Unexpected, "I am your chaos!")
            .with_operation("chaos")
            .set_temporary()
    }
}

impl<R: oio::ReadStream> oio::ReadStream for ChaosReader<R> {
    async fn read(&mut self) -> Result<Buffer> {
        if self.i_feel_lucky() {
            self.inner.read().await
        } else {
            Err(Self::unexpected_eof())
        }
    }
}

impl<R: oio::Read> oio::Read for ChaosReader<R> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        if self.i_feel_lucky() {
            let (rp, stream) = self.inner.open(range).await?;
            Ok((
                rp,
                Box::new(ChaosReader::new(stream, self.rng.clone(), self.error_ratio))
                    as Box<dyn oio::ReadStreamDyn>,
            ))
        } else {
            Err(Self::unexpected_eof())
        }
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        if self.i_feel_lucky() {
            self.inner.read(range).await
        } else {
            Err(Self::unexpected_eof())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll, Waker};

    use super::*;

    #[test]
    fn registered_chaos_requires_a_finite_ratio() {
        let registry = OperatorRegistry::get();
        register_chaos_layer(registry);
        let uri = "memory://?layers=chaos";
        let err = registry.load(uri).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        assert!(err.message().contains("error_ratio"));

        for value in [
            "",
            "-0.01",
            "1.01",
            "NaN",
            "inf",
            "-inf",
            "invalid-ratio-marker",
        ] {
            let err = registry
                .load((uri, [("layer.chaos.error_ratio", value)]))
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
            assert!(err.message().contains("error_ratio"));
            assert!(!err.to_string().contains("invalid-ratio-marker"));
            assert!(!format!("{err:?}").contains("invalid-ratio-marker"));
        }
    }

    #[test]
    fn registered_chaos_applies_ratio_boundaries() -> Result<()> {
        let registry = OperatorRegistry::get();
        register_chaos_layer(registry);
        let uri = "memory://?layers=chaos&layer.chaos.future_option=";
        let no_fault = registry.load((uri, [("layer.chaos.error_ratio", "0")]))?;
        let always_fault = registry.load((uri, [("layer.chaos.error_ratio", "1")]))?;

        let mut check = std::pin::pin!(async {
            no_fault.write("object", "data").await?;
            always_fault.write("object", "data").await?;
            assert_eq!(no_fault.read("object").await?.to_vec(), b"data");
            let err = always_fault.read("object").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_temporary());
            Ok(())
        });
        let Poll::Ready(result) = check.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("Memory operations should complete without waiting for I/O");
        };
        result
    }

    fn count_failures(error_ratio: f64, trials: usize) -> usize {
        let reader = ChaosReader::new(
            (),
            Arc::new(Mutex::new(StdRng::seed_from_u64(0))),
            error_ratio,
        );
        (0..trials).filter(|_| !reader.i_feel_lucky()).count()
    }

    #[test]
    fn test_sub_percent_error_ratio_injects_errors() {
        let failures = count_failures(0.005, 2_000);
        assert!(
            failures > 0 && failures < 2_000,
            "expected error_ratio=0.005 to inject some errors, got {failures}/2000"
        );
    }

    #[test]
    fn test_fractional_percent_error_ratio_not_truncated() {
        let failures_one_percent = count_failures(0.01, 2_000);
        let failures_one_point_five_percent = count_failures(0.015, 2_000);
        assert!(
            failures_one_point_five_percent > failures_one_percent,
            "expected error_ratio=0.015 ({failures_one_point_five_percent}/2000) to inject more errors than 0.01 ({failures_one_percent}/2000)"
        );
    }

    #[test]
    fn test_error_ratio_boundaries() {
        assert_eq!(count_failures(0.0, 1_000), 0);
        assert_eq!(count_failures(1.0, 1_000), 1_000);
    }
}
