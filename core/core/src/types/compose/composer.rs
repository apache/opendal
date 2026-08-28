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
    capability: Capability,
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
        let capability = srv.capability();
        let composer = srv.compose(&ctx, &to, args)?;

        Ok(Self {
            composer,
            to,
            scheme,
            capability,
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

        let input = input.into_compose_input();
        let path = normalize_path(&input.path);
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

        let mut version = input.version;
        let mut if_match = input.if_match;
        if let Some(metadata) = input.if_not_changed {
            if self.capability.compose_with_source_version
                && let Some(metadata_version) = metadata.version()
            {
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
            } else if self.capability.compose_with_source_if_match
                && let Some(metadata_etag) = metadata.etag()
            {
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
            } else if !self.capability.compose_with_source_version
                && !self.capability.compose_with_source_if_match
            {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!(
                        "The service {} does not support compose with source if_not_changed",
                        self.scheme
                    ),
                )
                .with_operation(Operation::Compose.into_static()));
            } else {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "if_not_changed metadata has no identity supported by compose",
                )
                .with_operation(Operation::Compose.into_static()));
            }
        }

        if version.is_some() && !self.capability.compose_with_source_version {
            return Err(Error::new(
                ErrorKind::Unsupported,
                format!(
                    "The service {} does not support compose with source version",
                    self.scheme
                ),
            )
            .with_operation(Operation::Compose.into_static()));
        }
        if if_match.is_some() && !self.capability.compose_with_source_if_match {
            return Err(Error::new(
                ErrorKind::Unsupported,
                format!(
                    "The service {} does not support compose with source if_match",
                    self.scheme
                ),
            )
            .with_operation(Operation::Compose.into_static()));
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
