- Proposal Name: `object_native_api`
- Start Date: 2022-02-18
- RFC PR: [datafuselabs/opendal#41](https://github.com/datafuselabs/opendal/pull/41)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Refactor API in object native way to make it easier to user.

# Motivation

`opendal` is not easy to use.

In our early adoption project `databend`, we can see a lot of code looks like:

```rust
let data_accessor = self.data_accessor.clone();
let path = self.path.clone();
let reader = SeekableReader::new(data_accessor, path.as_str(), stream_len);
let reader = BufReader::with_capacity(read_buffer_size as usize, reader);
Self::read_column(reader, &col_meta, data_type.clone(), arrow_type.clone()).await
```

And

```rust
op.stat(&path).run().await
```

## Conclusion

So in this proposal, I expect to address those problems. After implementing this proposal, we have a faster and easier-to-use `opendal`.

# Guide-level explanation

To operate on an object, we will use `Operator::object()` to create a new handler:

```rust
let o = op.object("path/to/file");
```

All operations that are available for `Object` for now includes:

- `metadata`: get object metadata (return an error if not exist).
- `delete`: delete an object.
- `reader`: create a new reader to read data from this object.
- `writer`: create a new writer to write data into this object.

Here is an example:

```rust
use anyhow::Result;
use futures::AsyncReadExt;

use opendal::services::fs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let op = Operator::new(fs::Backend::build().root("/tmp").finish().await?);

    let o = op.object("test_file");

    // Write data info file;
    let w = o.writer();
    let n = w
        .write_bytes("Hello, World!".to_string().into_bytes())
        .await?;
    assert_eq!(n, 13);

    // Read data from file;
    let mut r = o.reader();
    let mut buf = vec![];
    let n = r.read_to_end(&mut buf).await?;
    assert_eq!(n, 13);
    assert_eq!(String::from_utf8_lossy(&buf), "Hello, World!");

    // Get file's Metadata
    let meta = o.metadata().await?;
    assert_eq!(meta.content_length(), 13);

    // Delete file.
    o.delete().await?;

    Ok(())
}
```

# Reference-level explanation

We will provide a `Reader` (which implement both `AsyncRead + AsyncSeek`) for user instead of just a `AsyncRead`. In this `Reader`, we will:

- Not maintain internal buffer: caller can decide to wrap into `BufReader`.
- Only rely on accessor's `read` and `stat` operations.

To avoid the extra cost for `stat`, we will:

- Allow user specify total_size for `Reader`.
- Lazily Send `stat` while the first time `SeekFrom::End()`

To avoid the extra cost for `poll_read`, we will:

- Keep the underlying `BoxedAsyncRead` open, so that we can reuse the same connection/fd.

With these change, we can improve the `Reader` performance both on local fs and remote storage:

- fs, before

```rust
```

Other changes are just a re-order of APIs.

- `Operator::read() -> BoxedAsyncRead` => `Object::reader() -> Reader`
- `Operator::write(r: BoxedAsyncRead, size: u64)` => `Object::writer() -> Writer`
- `Operator::stat() -> Object` => `Object::stat() -> Metadata`
- `Operator::delete()` => `Object::delete()`

# Drawbacks

None.

# Rationale and alternatives

None

# Prior art

None

# Unresolved questions

None

# Future possibilities

- Implement `AsyncWrite` for `Writer` so that we can use `Writer` easier.
- Implement `Operator::objects()` to return an object iterator.
