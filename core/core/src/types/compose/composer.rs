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
use std::future::IntoFuture;

use crate::raw::oio::Compose;
use crate::raw::*;
use crate::*;

/// Creates one destination object from ordered complete source objects.
///
/// [`Composer::compose`] accepts sources incrementally and may start backend
/// work before returning. [`Composer::close`] waits for pending work and commits
/// the destination. A composer becomes terminal after an error.
pub struct Composer {
    composer: oio::Composer,
    to: String,
    scheme: &'static str,
    accepted: usize,
    metadata: Option<Metadata>,
    errored: bool,
}

impl Composer {
    pub(crate) fn create(
        ctx: OperationContext,
        srv: Servicer,
        to: String,
        args: OpCompose,
    ) -> Result<Self> {
        let scheme = srv.info().scheme();
        let composer = srv.compose(&ctx, &to, args)?;

        Ok(Self {
            composer,
            to,
            scheme,
            accepted: 0,
            metadata: None,
            errored: false,
        })
    }

    /// Accept one complete source object in sequence.
    ///
    /// A successful return means the source was accepted in order. Backend work
    /// may still be pending. This method applies backpressure when the
    /// configured concurrency window is full.
    pub async fn compose(&mut self, input: impl IntoComposeInput) -> Result<()> {
        self.compose_with(input).await
    }

    /// Accept one complete source object with additional options.
    ///
    /// Visit [`options::ComposeSourceOptions`] for all available options.
    pub fn compose_with(
        &mut self,
        input: impl IntoComposeInput,
    ) -> FutureComposeSource<'_, impl Future<Output = Result<()>> + '_> {
        let (path, options) = input.into_compose_input();
        FutureComposeSource {
            composer: self,
            path,
            options,
            f: Self::compose_inner,
        }
    }

    async fn compose_inner(
        composer: &mut Composer,
        path: String,
        options: options::ComposeSourceOptions,
    ) -> Result<()> {
        composer.compose_options(&path, options).await
    }

    /// Accept one complete source object with explicit options.
    ///
    /// Visit [`options::ComposeSourceOptions`] for all available options.
    pub async fn compose_options(
        &mut self,
        path: &str,
        options: options::ComposeSourceOptions,
    ) -> Result<()> {
        if self.errored {
            return Err(
                Error::new(ErrorKind::Unexpected, "composer has already failed")
                    .with_operation(Operation::Compose.into_static()),
            );
        }
        if self.metadata.is_some() {
            return Err(
                Error::new(ErrorKind::Unexpected, "composer is already closed")
                    .with_operation(Operation::Compose.into_static()),
            );
        }

        let path = normalize_path(path);
        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "source path is a directory")
                    .with_operation(Operation::Compose.into_static())
                    .with_context("service", self.scheme)
                    .with_context("path", path),
            );
        }
        if path == self.to {
            return Err(Error::new(
                ErrorKind::IsSameFile,
                "source and destination paths are same",
            )
            .with_operation(Operation::Compose.into_static())
            .with_context("service", self.scheme)
            .with_context("path", path));
        }

        let mut version = options.version;
        let mut if_match = options.if_match;
        if let Some(metadata) = options.if_not_changed {
            if let Some(metadata_version) = metadata.version() {
                if let Some(explicit) = version.as_deref() {
                    if explicit != metadata_version {
                        return Err(Error::new(
                            ErrorKind::ConditionNotMatch,
                            "if_not_changed conflicts with source version",
                        )
                        .with_operation(Operation::Compose.into_static()));
                    }
                } else {
                    version = Some(metadata_version.to_string());
                }
            } else if let Some(metadata_etag) = metadata.etag() {
                if let Some(explicit) = if_match.as_deref() {
                    if explicit != metadata_etag {
                        return Err(Error::new(
                            ErrorKind::ConditionNotMatch,
                            "if_not_changed conflicts with source if_match",
                        )
                        .with_operation(Operation::Compose.into_static()));
                    }
                } else {
                    if_match = Some(metadata_etag.to_string());
                }
            } else {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "if_not_changed metadata contains neither version nor ETag",
                )
                .with_operation(Operation::Compose.into_static()));
            }
        }

        let mut args = OpRead::new();
        if let Some(version) = version {
            args = args.with_version(&version);
        }
        if let Some(etag) = if_match {
            args = args.with_if_match(&etag);
        }

        if let Err(err) = self.composer.compose(&path, args).await {
            self.errored = true;
            return Err(err
                .with_operation(Operation::Compose.into_static())
                .with_context("service", self.scheme)
                .with_context("from", path)
                .with_context("to", &self.to));
        }
        self.accepted += 1;
        Ok(())
    }

    /// Commit all accepted sources and return destination metadata.
    ///
    /// Closing a composer with no accepted sources returns
    /// [`ErrorKind::ConfigInvalid`]. Repeated successful calls return the same
    /// metadata.
    pub async fn close(&mut self) -> Result<Metadata> {
        if self.errored {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "composer has already failed and can't be closed",
            )
            .with_operation(Operation::Compose.into_static()));
        }
        if let Some(metadata) = self.metadata.clone() {
            return Ok(metadata);
        }
        if self.accepted == 0 {
            self.errored = true;
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "compose requires at least one source object",
            )
            .with_operation(Operation::Compose.into_static())
            .with_context("service", self.scheme)
            .with_context("to", &self.to));
        }

        match self.composer.close().await {
            Ok(metadata) => {
                self.metadata = Some(metadata.clone());
                Ok(metadata)
            }
            Err(err) => {
                self.errored = true;
                Err(err
                    .with_operation(Operation::Compose.into_static())
                    .with_context("service", self.scheme)
                    .with_context("to", &self.to))
            }
        }
    }
}

/// [`Composer::compose_with`] returns this future.
///
/// Use its methods to configure one source object before awaiting it.
pub struct FutureComposeSource<'a, F: Future<Output = Result<()>>> {
    composer: &'a mut Composer,
    path: String,
    options: options::ComposeSourceOptions,
    f: fn(&'a mut Composer, String, options::ComposeSourceOptions) -> F,
}

impl<F: Future<Output = Result<()>>> FutureComposeSource<'_, F> {
    /// Compose this version of the source object.
    pub fn version(mut self, value: &str) -> Self {
        self.options.version = Some(value.to_string());
        self
    }

    /// Require this source ETag.
    pub fn if_match(mut self, value: &str) -> Self {
        self.options.if_match = Some(value.to_string());
        self
    }

    /// Require the source to retain the identity in `metadata`.
    pub fn if_not_changed(mut self, metadata: &Metadata) -> Self {
        self.options.if_not_changed = Some(metadata.clone());
        self
    }
}

impl<F: Future<Output = Result<()>>> IntoFuture for FutureComposeSource<'_, F> {
    type Output = Result<()>;
    type IntoFuture = F;

    fn into_future(self) -> Self::IntoFuture {
        let Self {
            composer,
            path,
            options,
            f,
        } = self;
        (f)(composer, path, options)
    }
}
