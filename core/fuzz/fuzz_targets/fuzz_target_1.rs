#![no_main]

use libfuzzer_sys::fuzz_target;
use opendal::Operator;
use uuid::Uuid;
use opendal::services::Fs;
use bytes::Bytes;
fuzz_target!(|data: &[u8]| {
    // fuzz data is a u8 slice, 
    // we need to convert it to a string
   // fuzz data is a u8 slice, 
    // we need to convert it to a string
    let data_bytes = Bytes::from(data.to_vec());
    let data_str = String::from_utf8_lossy(data);

    // Since we cannot use async in fuzz_target!
    // we need to use block_on to run async code

    let result:anyhow::Result<()> = tokio::runtime::Runtime::new().unwrap().block_on(
        async {
            let mut builder = Fs::default();
            builder.root("/Users/lqyue/githubProjects/opendal-test/opendal-test/tmp");
            let op: Operator = Operator::new(builder)?.finish();
            let file_name = format!("{}.txt", Uuid::new_v4());
            op.write(&file_name, data_bytes.clone()).await?;
            let bs = op.read(&file_name).await?;
            // assert_eq!(bs, data_str);
            op.delete(&file_name).await?;
            Ok(())
        }
    );



});
