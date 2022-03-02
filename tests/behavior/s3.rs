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
use opendal::Operator;
use opendal_test::services::s3;

use super::BehaviorTest;

#[tokio::test]
async fn test_s3() -> Result<()> {
    let acc = s3::new().await?;
    if acc.is_none() {
        println!("OPENDAL_S3_TEST not set, ignore");
        return Ok(());
    }
    let mut bh = BehaviorTest::new(Operator::new(acc.unwrap()));
    let metrics = bh.run().await?;

    let acc = bh.op.inner();
    let acc_metrics = acc.metrics();

    println!("{:?}, {:?}", metrics, acc_metrics);

    // TODO: after https://github.com/datafuselabs/opendal/issues/86 is fixed
    // assert_eq!(format!("{:?}", metrics), format!("{:?}", acc_metrics));
    Ok(())
}
