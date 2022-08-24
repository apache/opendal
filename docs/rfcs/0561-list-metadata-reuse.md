- Proposal Name: `list_metadata_reuse`
- Start Date: 2022-08-23
- RFC PR: [datafuselabs/opendal#561](https://github.com/datafuselabs/opendal/pull/561)
- Tracking Issue: None

# Summary

Reuse metadata returned during listing, by defining a new `MetaLite` struct storing some metadata fields, and embedding it to `DirEntry`.

# Motivation

Users may expect to browse metadata of some directories' child files and directories. `list()` seems to be an ideal way to complete this job. 

Thus, they start iterating on it, but soon they realized the `DirEntry`, which could only offer the name (or path, more precisely) and access mode of the object.

So they have to call `metadata()` for each name they extracted from the iterator.

The final example looks like:

```rust
let op = Operator::from_env(Scheme::Gcs)?.batch();

// here is a network request
let mut dir_stream = op.walk("/dir/to/walk")?;

while let Some(Ok(file)) = dir_stream.next().await {
    let path = file.path();

    // here is another network request
    let size = file.metadata().await?.content_length();
    println!("size of file {} is {}B", path, size);
}
```

But...wait! many storage-services returns object metadata when listing, like HDFS, AWS and GCS. The rust standard library returns metadata when listing local file systems, too.

In the previous versions of OpenDAL we just simply ignored them. This wastes users' time on requesting on metadata.

# Guide-level explanation

The loop in main will be changed to the following code with this RFC:

```rust
while let Some(Ok(file)) = dir_stream.next().await {
    let size = if let Some(len) = file.content_length() {
        len
    } else {
        file.metadata().await?.content_length();
    };
    let name = file.path();
    println!("size of file {} is {}B", path, size);
}

```

# Reference-level explanation

This RFC suggests embedding some metadata fields in `DirEntry`

```rust
struct MetaLite {
    pub content_length: u64,  // size of file
    pub last_modified: OffsetDateTime,
    pub created: OffsetDateTime,    // time created
}
pub struct DirEntry {
    acc: Arc<dyn Accessor>,
    
    mode: ObjectMode,
    path: String,
    
    // newly add metadata struct
    metadata: Option<MetaLite>,
}

impl DirEntry {
    // get size of file
    pub fn content_length(&self) -> Option<u64> {
        self.metadata.as_ref().map(|m| m.content_length)
    }
    // get the last modified time
    pub fn last_modified(&self) -> Option<OffsetDateTime> {
        self.metadata.as_ref().map(|m| m.last_modified)
    }
    // get the create time
    pub fn created(&self) -> Option<OffsetDateTime> {
        self.metadata.as_ref().map(|m| m.created)
    }
}
```

For all services that supplies metadata during listing, like AWS, GCS and HDFS. Those optional fields will be filled up; Meanwhile for those services doesn't return metadata during listing, like in memory storages, just left them as `None`.

As you can see, for those services returning metadata when listing, the operation of listing metadata will save many unnecessary requests.

# Drawbacks
 
Add complexity to `DirEntry`. To use the improved features of `DirEntry`, users have to explicitly check the existence of metadata fields.

The size of `DirEntry` increased from 40 bytes to 80 bytes, a 100% percent growth requires more memory.

# Rational and alternatives

The largest drawback of performance usually comes from network or hard disk operations. By adding an `MetaLite` field in `DirEntry`, we could avoid many redundant requests.

## alternative implementation

Simply extend `DirEntry`:

```rust
pub struct DirEntry {
    acc: Arc<dyn Accessor>,

    mode: ObjectMode,
    path: String,

    // newly add metadata fields
    content_length: Option<u64>,  // size of file
    last_modified: Option<u64>,
    created: Option<u64>,    // time created
}
```

The reason why not preferred is that the existence of those newly added metadata fields is highly correlated. If one field does not exist, the others neither.

By wrapping them together in an embedded structure, we can save 8 bytes of space for each `DirEntry` object. In the future, more metadata fields may be added to `DirEntry`, then a lot more space could be saved.

# Prior art

None.

# Unresolved questions

None.

# Future possibilities

## More Fields

Add more metadata fields to DirEntry, like:

- accessed: the last access timestamp of object

## Simplified Get

Users have to explicitly check if those metadata fields actual present in the DirEntry. This may be done inside the getter itself.

```rust
let path = file.path();

// if content_length is not exist
// this getter will automatically fetch from the storage service.
let size = file.content_length().await?;

// the previous getter can cache metadata fetched from service
// so this function could return instantly.
let created = file.created().await?;
println!("size of file {} is {}B, created at {}", path, size, created);
```
