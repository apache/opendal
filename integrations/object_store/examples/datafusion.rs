use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::prelude::*;
use opendal::services::S3;
use opendal::Operator;
use std::sync::Arc;
use url::Url;

/// This example demonstrates querying data in an S3 bucket using DataFusion and OpenDAL.
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Configure OpenDAL for S3
    let region = "my_region";
    let bucket_name = "my_bucket";
    let builder = S3::default()
        .endpoint("my_endpoint")
        .bucket(bucket_name)
        .region(region)
        .access_key_id("my_access_key")
        .secret_access_key("my_secret_key");
    let op = Operator::new(builder)
        .map_err(|err| DataFusionError::External(Box::new(err)))?
        .finish();
    let store = object_store_opendal::OpendalStore::new(op);

    // Register the object store
    let path = format!("s3://{bucket_name}");
    let s3_url = Url::parse(&path).unwrap();
    ctx.register_object_store(&s3_url, Arc::new(store));

    // Register CSV file as a table
    let path = format!("s3://{bucket_name}/csv/aggregate_test_100.csv");
    ctx.register_csv("trips", &path, CsvReadOptions::default())
        .await?;

    // Execute the query
    let df = ctx.sql("SELECT * FROM trips LIMIT 10").await?;
    // Print the results
    df.show().await?;

    // Dynamic query using the file path directly
    let ctx = ctx.enable_url_table();
    let df = ctx
        .sql(format!(r#"SELECT * FROM '{}' LIMIT 10"#, &path).as_str())
        .await?;
    // Print the results
    df.show().await?;

    Ok(())
}
