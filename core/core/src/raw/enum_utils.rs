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

//! [`type_alias_impl_trait`](https://github.com/rust-lang/rust/issues/63063) is not stable yet.
//! So we can't write the following code:
//!
//! ```txt
//! impl Service for S3Backend {
//!     fn write(...) -> Result<oio::Writer>;
//! }
//! ```
//!
//! Which means we have to write the type directly like:
//!
//! ```txt
//! impl Service for OssBackend {
//!     let writer = raw::TwoWays<
//!         oio::MultipartWriter<OssWriter>,
//!         oio::AppendWriter<OssWriter>,
//!     >;
//! }
//! ```
//!
//! This module is used to provide some enums for the above code. We should remove this module once
//! type_alias_impl_trait has been stabilized.

use crate::raw::*;
use crate::*;

/// TwoWays is used to implement traits that based on two ways.
///
/// Users can wrap two different trait types together.
pub enum TwoWays<ONE, TWO> {
    /// The first type for the [`TwoWays`].
    One(ONE),
    /// The second type for the [`TwoWays`].
    Two(TWO),
}

impl<ONE: oio::ReadStream, TWO: oio::ReadStream> oio::ReadStream for TwoWays<ONE, TWO> {
    async fn read(&mut self) -> Result<Buffer> {
        match self {
            TwoWays::One(v) => v.read().await,
            TwoWays::Two(v) => v.read().await,
        }
    }
}

impl<ONE: oio::Write, TWO: oio::Write> oio::Write for TwoWays<ONE, TWO> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        match self {
            Self::One(v) => v.write(bs).await,
            Self::Two(v) => v.write(bs).await,
        }
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        match self {
            Self::One(v) => v.copy_from(path, args, range).await,
            Self::Two(v) => v.copy_from(path, args, range).await,
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        match self {
            Self::One(v) => v.close().await,
            Self::Two(v) => v.close().await,
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match self {
            Self::One(v) => v.abort().await,
            Self::Two(v) => v.abort().await,
        }
    }
}

impl<ONE: oio::Copy, TWO: oio::Copy> oio::Copy for TwoWays<ONE, TWO> {
    async fn next(&mut self) -> Result<Option<usize>> {
        match self {
            Self::One(v) => v.next().await,
            Self::Two(v) => v.next().await,
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        match self {
            Self::One(v) => v.close().await,
            Self::Two(v) => v.close().await,
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match self {
            Self::One(v) => v.abort().await,
            Self::Two(v) => v.abort().await,
        }
    }
}

impl<ONE: oio::List, TWO: oio::List> oio::List for TwoWays<ONE, TWO> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self {
            Self::One(v) => v.next().await,
            Self::Two(v) => v.next().await,
        }
    }
}

/// ThreeWays is used to implement traits that based on three ways.
///
/// Users can wrap three different trait types together.
pub enum ThreeWays<ONE, TWO, THREE> {
    /// The first type for the [`ThreeWays`].
    One(ONE),
    /// The second type for the [`ThreeWays`].
    Two(TWO),
    /// The third type for the [`ThreeWays`].
    Three(THREE),
}

impl<ONE: oio::ReadStream, TWO: oio::ReadStream, THREE: oio::ReadStream> oio::ReadStream
    for ThreeWays<ONE, TWO, THREE>
{
    async fn read(&mut self) -> Result<Buffer> {
        match self {
            ThreeWays::One(v) => v.read().await,
            ThreeWays::Two(v) => v.read().await,
            ThreeWays::Three(v) => v.read().await,
        }
    }
}

impl<ONE: oio::Write, TWO: oio::Write, THREE: oio::Write> oio::Write
    for ThreeWays<ONE, TWO, THREE>
{
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        match self {
            Self::One(v) => v.write(bs).await,
            Self::Two(v) => v.write(bs).await,
            Self::Three(v) => v.write(bs).await,
        }
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        match self {
            Self::One(v) => v.copy_from(path, args, range).await,
            Self::Two(v) => v.copy_from(path, args, range).await,
            Self::Three(v) => v.copy_from(path, args, range).await,
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        match self {
            Self::One(v) => v.close().await,
            Self::Two(v) => v.close().await,
            Self::Three(v) => v.close().await,
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match self {
            Self::One(v) => v.abort().await,
            Self::Two(v) => v.abort().await,
            Self::Three(v) => v.abort().await,
        }
    }
}

impl<ONE: oio::List, TWO: oio::List, THREE: oio::List> oio::List for ThreeWays<ONE, TWO, THREE> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self {
            Self::One(v) => v.next().await,
            Self::Two(v) => v.next().await,
            Self::Three(v) => v.next().await,
        }
    }
}

/// FourWays is used to implement traits that based on four ways.
///
/// Users can wrap four different trait types together.
pub enum FourWays<ONE, TWO, THREE, FOUR> {
    /// The first type for the [`FourWays`].
    One(ONE),
    /// The second type for the [`FourWays`].
    Two(TWO),
    /// The third type for the [`FourWays`].
    Three(THREE),
    /// The fourth type for the [`FourWays`].
    Four(FOUR),
}

impl<ONE, TWO, THREE, FOUR> oio::ReadStream for FourWays<ONE, TWO, THREE, FOUR>
where
    ONE: oio::ReadStream,
    TWO: oio::ReadStream,
    THREE: oio::ReadStream,
    FOUR: oio::ReadStream,
{
    async fn read(&mut self) -> Result<Buffer> {
        match self {
            FourWays::One(v) => v.read().await,
            FourWays::Two(v) => v.read().await,
            FourWays::Three(v) => v.read().await,
            FourWays::Four(v) => v.read().await,
        }
    }
}

impl<ONE, TWO, THREE, FOUR> oio::List for FourWays<ONE, TWO, THREE, FOUR>
where
    ONE: oio::List,
    TWO: oio::List,
    THREE: oio::List,
    FOUR: oio::List,
{
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self {
            Self::One(v) => v.next().await,
            Self::Two(v) => v.next().await,
            Self::Three(v) => v.next().await,
            Self::Four(v) => v.next().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[derive(Default)]
    struct MockWriter {
        copied: Vec<(String, Option<String>, BytesRange)>,
    }

    impl oio::Write for MockWriter {
        async fn write(&mut self, _: Buffer) -> Result<()> {
            Ok(())
        }

        async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
            self.copied
                .push((path.to_string(), args.version().map(str::to_owned), range));
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_three_ways_write_copy_from_delegates() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (_, args, _) = OpRead::from_options(
                &Capability {
                    read_with_version: true,
                    ..Default::default()
                },
                options::ReadOptions {
                    version: Some("v1".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
            let range = BytesRange::new(1, Some(4));

            let mut one: ThreeWays<MockWriter, MockWriter, MockWriter> =
                ThreeWays::One(MockWriter::default());
            oio::Write::copy_from(&mut one, "src-one", args.clone(), range)
                .await
                .unwrap();
            let ThreeWays::One(inner) = one else {
                unreachable!()
            };
            assert_eq!(
                inner.copied,
                vec![("src-one".to_string(), Some("v1".to_string()), range)]
            );

            let mut two: ThreeWays<MockWriter, MockWriter, MockWriter> =
                ThreeWays::Two(MockWriter::default());
            oio::Write::copy_from(&mut two, "src-two", args.clone(), range)
                .await
                .unwrap();
            let ThreeWays::Two(inner) = two else {
                unreachable!()
            };
            assert_eq!(
                inner.copied,
                vec![("src-two".to_string(), Some("v1".to_string()), range)]
            );

            let mut three: ThreeWays<MockWriter, MockWriter, MockWriter> =
                ThreeWays::Three(MockWriter::default());
            oio::Write::copy_from(&mut three, "src-three", args, range)
                .await
                .unwrap();
            let ThreeWays::Three(inner) = three else {
                unreachable!()
            };
            assert_eq!(
                inner.copied,
                vec![("src-three".to_string(), Some("v1".to_string()), range)]
            );
        })
        .await
        .expect("test_three_ways_write_copy_from_delegates timed out");
    }
}
