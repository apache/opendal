use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};

use opendal::{services::S3Config, Operator};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, AsyncArrowWriter};
use parquet_opendal::AsyncWriter;

#[tokio::main]
async fn main() {
    let mut cfg = S3Config::default();
    cfg.access_key_id = Some("my_access_key".to_string());
    cfg.secret_access_key = Some("my_secret_key".to_string());
    cfg.endpoint = Some("my_endpoint".to_string());
    cfg.region = Some("my_region".to_string());
    cfg.bucket = "my_bucket".to_string();

    // Create a new operator
    let operator = Operator::from_config(cfg).unwrap().finish();
    let path = "/path/to/file.parquet";

    // Create an async writer
    let writer = AsyncWriter::new(
        operator
            .writer_with(path)
            .chunk(32 * 1024 * 1024)
            .concurrent(8)
            .await
            .unwrap(),
    );

    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
    let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), None).unwrap();
    writer.write(&to_write).await.unwrap();
    writer.close().await.unwrap();

    let buffer = operator.read(path).await.unwrap().to_bytes();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(buffer)
        .unwrap()
        .build()
        .unwrap();
    let read = reader.next().unwrap().unwrap();
    assert_eq!(to_write, read);
}
