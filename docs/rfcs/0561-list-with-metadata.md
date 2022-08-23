- Proposal Name: `list_with_metadata`
- Start Date: 2022-08-23
- RFC PR: None
- Tracking Issue: None

# Summary

Add a `metadata` field for `DirEntry`s.

```rust
pub struct DirEntry {
    acc: Arc<dyn Accessor>,

    metadata: ObjectMetadata,
    path: String,
}

impl DirEntry {
    // get size of file
    pub fn len(&self) -> u64 {
        self.metadata.content_length()
    }
    // get the last modified time
    pub fn modified(&self) -> DateTimeOffset {
        self.metadata.last_modified()
    }
    // other metadata getters
}
```

# Motivation

Users may expect to browse metadata of some directories' child files and directories.
`list()` seems to be an ideal way to complete this job. 

Thus, they start iterating on it, but soon they realized the `DirEntry`, which could only offer the name (or path, 
more precisely) and access mode of the object.

So they have to call `meta()` for each name they extracted from the iterator.
```
// Pesudo code example of listing size of files
for file in list().await {
    let name = file.path
    let size = object(name).meta().await.file_size()
}
```

But...wait! many storage-services returns object metadata when listing, like HDFS, AWS and GCS. The rust standard library
returns metadata when listing local file systems, too.

In the previous versions of OpenDAL we just simply ignored them. This wastes users' time on requesting on metadata.

If equip `DirEntry` with metadata, code could be simplified to:
```
// Pesudo code example of listing size of files
for file in list().await {
    let size = file.len()
}
```
It's much simpler and requires less network or hard drive reading requests.

# Benefits

As you can see, the operation of listing metadata is simplified and saves many unnecessary requests.

# Drawbacks
 
The in memory storage does not return metadata when listing, this requires getting metadata for each child file 
and directory, leading to longer time holding on locks.

---
Thanks to @sandflee