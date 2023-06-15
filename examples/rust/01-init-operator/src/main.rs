use std::collections::HashMap;

use opendal::services::S3;
use opendal::Operator;
use opendal::Result;
use opendal::Scheme;

fn main() -> Result<()> {
    let op = init_operator_via_builder()?;
    println!("operator from builder: {:?}", op);

    let op = init_operator_via_map()?;
    println!("operator from map: {:?}", op);

    Ok(())
}

fn init_operator_via_builder() -> Result<Operator> {
    let mut builder = S3::default();
    builder.bucket("example");
    builder.region("us-east-1");
    builder.access_key_id("access_key_id");
    builder.secret_access_key("secret_access_key");

    let op = Operator::new(builder)?.finish();
    Ok(op)
}

fn init_operator_via_map() -> Result<Operator> {
    let mut map = HashMap::default();
    map.insert("bucket".to_string(), "example".to_string());
    map.insert("region".to_string(), "us-east-1".to_string());
    map.insert("access_key_id".to_string(), "access_key_id".to_string());
    map.insert(
        "secret_access_key".to_string(),
        "secret_access_key".to_string(),
    );

    let op = Operator::via_map(Scheme::S3, map)?;
    Ok(op)
}
