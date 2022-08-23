- Proposal Name: `list_metadata_reuse`
- Start Date: 2022-08-23
- RFC PR: [datafuselabs/opendal#561](https://github.com/datafuselabs/opendal/pull/561)
- Tracking Issue: None

# Summary

Add a `metadata` field for `DirEntry`s.

# Motivation

Users may expect to browse metadata of some directories' child files and directories.
`list()` seems to be an ideal way to complete this job. 

Thus, they start iterating on it, but soon they realized the `DirEntry`, which could only offer the name (or path, 
more precisely) and access mode of the object.

So they have to call `meta()` for each name they extracted from the iterator.
```
// Pseudo code example of listing size of files
for file in list().await {
    let name = file.path
    let size = object(name).meta().await.file_size()
}
```

But...wait! many storage-services returns object metadata when listing, like HDFS, AWS and GCS. The rust standard library
returns metadata when listing local file systems, too.

In the previous versions of OpenDAL we just simply ignored them. This wastes users' time on requesting on metadata.

# Guide-level explanation

The pseudocode will be changed to the following code with this RFC:
```
// Pseudo code example of listing size of files
for file in list().await {
    let size = if let Some(size) = file.len() {
        size
    } else {
        let name = file.path;
        object(name).meta().await.file_size()
    }
}
```
It's much simpler and requires less network or hard drive reading requests.

# Reference-level explanation

We will embed an ObjectMetadata object in DirEntry, and set 
```rust
pub struct DirEntry {
    acc: Arc<dyn Accessor>,
    
    mode: ObjectMode,
    path: String,
    
    // newly add metadata
    content_length: Option<u64>,
    last_modified: Option<OffsetDateTime>,
    created: Option<OffsetDateTime>,
    
}

impl DirEntry {
    // get size of file
    pub fn len(&self) -> Option<u64> {
        self.content_length
    }
    // get the last modified time
    pub fn modified(&self) -> Option<OffsetDateTime> {
        self.last_modified
    }
    // get the create time
    pub fn created(&self) -> Option<OffsetDateTime> {
        self.created
    }
}
```

For all services that supplies metadata during listing, like AWS, GCS and HDFS.
We will fill those optional fields up; Meanwhile for those services doesn't return metadata during listing,
like in memory storages, just left them as `None`.

# Benefits

As you can see, the operation of listing metadata will save many unnecessary requests.

# Drawbacks
 
None.

# Rational and alternatives

None.

# Prior art

None.

# Unresolved questions

None.

# Future possibilities

None.
