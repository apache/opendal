// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.



use anyhow::Result;
use log::warn;
use opendal::Operator;
use opendal_test::services::s3;

use super::BehaviorTest;
use super::trace::jaeger_tracing;
#[tokio::test]
async fn behavior() -> Result<()> {
    super::init_logger();

    let acc = s3::new().await?;
    if acc.is_none() {
        warn!("OPENDAL_S3_TEST not set, ignore");
        return Ok(());
    }

    jaeger_tracing(
        BehaviorTest::new(Operator::new(acc.unwrap())).run(),
        "127.0.0.1:6831",
        "opendal_trace",
    )
    .await?;
    Ok(())
}
