- Proposal Name: `merge_range_read_into_read`
- Start Date: 2023-08-20
- RFC PR: [apache/incubator-opendal#2884](https://github.com/apache/incubator-opendal/pull/2884)
- Tracking Issue: [apache/incubator-opendal#2811](https://github.com/apache/incubator-opendal/issues/2811)

# Summary

Merge the `range_read` API into `read` by deleting the `op.range_reader(path, range)` and `op.range_read(path, range)` method.

# Motivation

Currently OpenDAL has separate `appender` and `writer` APIs:

```rust
let mut appender = op.appender_with("file.txt").await?; 

appender.append(bs).await?;
appender.append(bs).await?;
```

This duplication forces users to learn two different APIs for writing data.

By adding this change, we can:

- Simpler API surface - users only need to learn one writing API.
- Reduce code duplication between append and write implementations.
- Atomic append semantics are handled internally in `writer`.
- Reuse the `sink` api for both `overwrite` and `append` mode.

# Guide-level explanation

The new approach is to enable append mode on `writer`:

```rust 
let mut writer = op.writer_with("file.txt").append(true).await?;

writer.write(bs).await?; // appends to file
writer.write(bs).await?; // appends again
```

Calling `writer_with.append(true)` will start the writer in append mode. Subsequent `write()` calls will append rather than overwrite.

There is no longer a separate `appender` API.

# Reference-level explanation

We will add an `append` flag into `OpWrite`:

```rust
impl OpWrite {   
    pub fn with_append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }
}
```

All services need to check `append` flag and handle append mode accordingly. Services that not support append should return an `Unsupported` error instead.

# Drawbacks

- `writer` API is more complex with the append mode flag.
- Internal implementation must handle both overwrite and append logic.

# Rationale and alternatives

None

# Prior art

Python's file open() supports an `"a"` mode flag to enable append-only writing.

# Unresolved questions

None

# Future possibilities

None
