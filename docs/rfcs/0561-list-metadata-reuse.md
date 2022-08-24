- Proposal Name: `list_metadata_reuse`
- Start Date: 2022-08-23
- RFC PR: [datafuselabs/opendal#561](https://github.com/datafuselabs/opendal/pull/561)
- Tracking Issue: None

# Summary

Extend `DirEntry` with more `metadata` fields.

# Motivation

Users may expect to browse metadata of some directories' child files and directories. `list()` seems to be an ideal way to complete this job. 

Thus, they start iterating on it, but soon they realized the `DirEntry`, which could only offer the name (or path, more precisely) and access mode of the object.

So they have to call `metadata()` for each name they extracted from the iterator.

The final example looks like:

```rust
use anyhow::Result;
use futures_lite::StreamExt;
use opendal::ops::OpList;
use opendal::ops::OpStat;
use opendal::services::gcs::Builder;
use opendal::Accessor;

#[tokio::main]
async fn main() -> Result<()> {
    let list_op = OpList::new("/path/to/dir")?;
    let accessor = Builder::default()
        .bucket("example")
        .root("/example/root/")
        .credential("example-credential")
        .build()?;

    // here is a network request
    let mut dir_stream = accessor.list(&list_op).await?;

    while let Some(Ok(file)) = dir_stream.next().await {
        let path = file.path();
        let stat_op = OpStat::new(path)?;

        // here is another network request
        let size = accessor.stat(&stat_op).await?.content_length();

        println!("size of file {} is {}B", path, size);
    }
    Ok(())
}
```

But...wait! many storage-services returns object metadata when listing, like HDFS, AWS and GCS. The rust standard library
returns metadata when listing local file systems, too.

In the previous versions of OpenDAL we just simply ignored them. This wastes users' time on requesting on metadata.

# Guide-level explanation

The loop in main will be changed to the following code with this RFC:
```rust
while let Some(Ok(file)) = dir_stream.next().await {
    let size = file.len().await;

    println!("size of file {} is {}B", path, size);
}
```

# Reference-level explanation

This RFC suggests embedding some metadata fields in `DirEntry`
```rust
pub struct DirEntry {
    acc: Arc<dyn Accessor>,
    
    mode: ObjectMode,
    path: String,
    
    // newly add metadata fields
    content_length: Option<u64>,  // size of file
    last_modified: Option<OffsetDateTime>,
    created: Option<OffsetDateTime>,    // time created
}

impl DirEntry {
    // get size of file
    pub fn content_length(&self) -> Option<u64> {
        self.content_length
    }
    // get the last modified time
    pub fn last_modified(&self) -> Option<OffsetDateTime> {
        self.last_modified
    }
    // get the create time
    pub fn created(&self) -> Option<OffsetDateTime> {
        self.created
    }
}
```

For all services that supplies metadata during listing, like AWS, GCS and HDFS. Those optional fields will be filled up; Meanwhile for those services doesn't return metadata during listing, like in memory storages, just left them as `None`.

# Benefits

As you can see, for those services returning metadata when listing, the operation of listing metadata will save many unnecessary requests.

# Drawbacks
 
More code required if you want to reach better performance.

# Rational and alternatives

The largest drawback of performance usually comes from network or hard disk operations. By extending the fields of DirEntry, we could avoid many redundant requests.

# Prior art

None.

# Unresolved questions

None.

# Future possibilities

## More Fields
Add more metadata fields to DirEntry, like:

- accessed: the last access timestamp of object

## Simplified Get
Users have to explicitly check if those metadata fields actual present in the DirEntry. This could be done inside their getters.
