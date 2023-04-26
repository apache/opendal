- Proposal Name: `introduce_append_api`
- Start Date: 2023-04-26
- RFC PR: 
- Tracking Issue: 

# Summary

Separate append operations from the write operation.

# Motivation

OpenDAL has the write operation used to create a file and append data to it. This is implemented based on multipart API. However, current approach has some limitations:

- Data could be lost and not readable before w.close() returned Ok(())
- File can't be appended again after w.close() returned Ok(())

To address these issues, I propose separating the write and append operations. The new write operation will not be reentrant, but users can create a writer that provides a reentrant append operation.

# Guide-level explanation

The files created by the write operation can not be modified, only overwritten.

```rust
async fn write_test(op: Operation) -> Result<()> {
    let bs = read_from_file();
    op.write("path_to_file", bs).await?;
  
    // The following code will report an error
    op.write("path_to_file", bs).await?;
  
    // Unless specified can overwrite
    let option = OpWrite::default().with_allow_overwrite(true);
    op.write_with("path_to_file", option, bs).await?;
}
```

The files created by the append operation can be appended via multipart API or append API.

```rust
async fn append_test(op: Operation) -> Result<()> {
    // create writer
    let writer = op.writer("path_to_file").await?;
  
    let bs = read_from_file();
    writer.append(bs).await?;
  
    let bs = read_from_another_file();
    writer.append(bs).await?;
  
    // close the file
    // for multipart API, complete the upload and prevent writing after close
    // for native append API, this is a no-op
    writer.close().await?;
  
    // abort the operation and delete all uploaded data
    writer.abort().await?;
}
```

# Reference-level explanation

To implement this feature, we need to add a new API `append` into `oio::Writer`. And change the `oio::Writer::write` to be not reentrant.

```rust
#[async_trait]
pub trait Write: Unpin + Send + Sync {
    /// Write given into writer. This operation will overwrite the file if it is supported.
    async fn write(&mut self, bs: Bytes) -> Result<()>;
  
    /// Append data to the end of file.
    /// Users will call `append` multiple times. Please make sure `append` is safe to re-enter.
    async fn append(&mut self, bs: Bytes) -> Result<()>;

    /// Abort the pending writer.
    async fn abort(&mut self) -> Result<()>;

    /// Close the writer and make sure all data has been flushed.
    async fn close(&mut self) -> Result<()>;
}
```

# Drawbacks

None.

# Rationale and alternatives

This is a break change. It changes the semantics of write and adds append API.

# Prior art

None.

# Unresolved questions

None.

# Future possibilities

We can use append API instead of multipart API for services that natively support append, such as [Azure blob](https://learn.microsoft.com/en-us/rest/api/storageservices/append-block?tabs=azure-ad) and [Alibaba cloud OSS](https://www.alibabacloud.com/help/en/object-storage-service/latest/appendobject). This will improve the performance and reliability of append operation.
