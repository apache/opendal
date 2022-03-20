use anyhow::Result;
use log::warn;
use opendal::Operator;
use opendal_test::services::azblob;

use super::BehaviorTest;

#[tokio::test]
async fn behavior() -> Result<()> {
    super::init_logger();

    let acc = s3::new().await?;
    if acc.is_none() {
        warn!("OPENDAL_S3_TEST not set, ignore");
        return Ok(());
    }

    BehaviorTest::new(Operator::new(acc.unwrap())).run().await
}