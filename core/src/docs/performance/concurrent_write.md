# Concurrent Write

OpenDAL writes data sequentially by default.

```rust
# use opendal::Operator;
# async fn test() {
let w = op.writer("test.txt").await?;
w.write(data1).await?;
w.write(data2).await?;
w.close().await?;
# }
```

Most of the time, this can't maximize write performance due to limitations on a single connection. We can perform concurrent writes to improve performance.

```rust
# use opendal::Operator;
# async fn test() {
let w = op
    .writer_with("test.txt")
    .concurrent(8)
    .await?;

w.write(data1).await?;
w.write(data2).await?;
w.close().await?;
# }
```

After setting `concurrent`, OpenDAL will attempt to write the specified file concurrently. The maximum level of concurrency is determined by the `concurrent` parameter. By default, it is set to 1, indicating a sequential write operation.

Under the hood, OpenDAL maintains a task queue to manage concurrent writes. It spawns asynchronous tasks in the background using the [`Executor`][crate::Executor] and tracks the status of each task. The task queue is flushed when the writer is closed, allowing data to be written concurrently without blocking the main thread.

Take our example here, the write of `data1` will not block the write of `data2`. The two writes will be executed concurrently.

The underlying implementation of concurrent writes may vary depending on the backend. For instance, the `s3` backend leverages the S3 Multipart Uploads API to handle concurrent writes, while the `azblob` backend utilizes the Block API for the same purpose.

## Tuning

There are two parameters that can be tuned to optimize concurrent writes:

- `concurrent`: This parameter controls the maximum number of concurrent writes. The default value is 1.
- `chunk`: This parameter specifies the size of each chunk of data to be written. The default value is vary for different storage services.

### `concurrent`

The most important thing to understand is that `concurrent` is not a strict limit. It represents the maximum number of concurrent writes that OpenDAL will attempt to perform. The actual number of concurrent writes may be lower, depending on the input data throughput.

For example, if you set `concurrent` to 8, OpenDAL will attempt to perform up to 8 concurrent writes. However, if the input data throughput is low, it might only carry out 2 or 3 concurrent writes at a time, as there isn't enough data to keep all 8 writes active.

The best value for `concurrent` depends on the specific use case and the underlying storage service. In general, a higher value can lead to better performance, but it highly depends on the storage service and the network conditions. For example, if the storage service is robust and bandwidth is sufficient, you may observe a linear increase in performance with higher `concurrent` values. However, if the storage service has request limits or the network is nearly saturated, increasing `concurrent` may not lead to any performance improvement—and could even degrade performance due to infinite retries on errors.

It's recommended to start with a lower value like `2` or `4` and gradually increase it while monitoring performance and resource usage.

### `chunk`

The `chunk` parameter specifies the size of each chunk of data to be written. A larger chunk size can improve performance, but it may also increase memory usage. The default value is vary for different storage services. 

For example, s3 is using `5MiB` as the default chunk size. It's also the minimum chunk size for s3. If you set a smaller chunk size, OpenDAL will automatically adjust it to `5MiB`.

The best value for `chunk` depends on the specific use case and the underlying storage service. For most object storage services, a chunk size of `8MiB` or larger is recommended. However, if you're working with smaller files or have limited memory resources, you may want to use a smaller chunk size.

Please note that if you input small chunks of data, OpenDAL will attempt to merge them into a larger chunk before writing. This helps avoid the overhead of writing numerous small chunks, which can negatively affect performance.

## Usage

To upload a large in-memory chunk concurrently:

```rust
# use opendal::Operator;
# async fn test() {
let data = vec![0; 10 * 1024 * 1024]; // 10MiB
let _ = op.write_with("test.txt", data).concurrent(4).await?;
# }
```

`concurrent` and `chunk` also works in [`into_sink`][crate::Writer::into_sink], [`into_bytes_sink`][crate::Writer::into_bytes_sink] and [`into_futures_async_write`][crate::Writer::into_futures_async_write]:

```rust
use std::io;

use bytes::Bytes;
use futures::SinkExt;
use opendal::{Buffer, Operator};
use opendal::Result;

async fn test(op: Operator) -> io::Result<()> {
    let mut w = op
        .writer_with("hello.txt")
        .concurrent(8)
        .chunk(256)
        .await?
        .into_sink();
    let bs = "Hello, World!".as_bytes();
    w.send(Buffer::from(bs)).await?;
    w.close().await?;

    Ok(())
}
```
