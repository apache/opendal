- Proposal Name: `backend_native_api`
- Start Date: 2022-02-18
- RFC PR: [datafuselabs/opendal#41](https://github.com/datafuselabs/opendal/pull/41)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Refactor API in backend native way to archive the performance.

# Motivation

## Poor performance

`opendal` is quite slow on `fs`: [performance drop 3 times after bump up opendal](https://github.com/datafuselabs/databend/issues/4197)

First, in every `read` operation, we will do at least three syscall: `open`, `seek`, `read`.

```rust
let mut f = fs::OpenOptions::new()
    .read(true)
    .open(&path)
    .await
    .map_err(|e| parse_io_error(&e, &path))?;

if let Some(offset) = args.offset {
    f.seek(SeekFrom::Start(offset))
        .await
        .map_err(|e| parse_io_error(&e, &path))?;
};
```

To make everything worse, our `SeelableReader` is designed for object storage systems, and works much slower on local fs.

`SeelableReader` will try to prefetch data in memory and maintain an internal pointer to implement `AsyncSeek`. However, due to our poor (nearly no) optimization, `SeekableReader` is much slower than using system call `seek`.

## Ergonomic unfriendly

`opendal` is not easy to use either.

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

So in this proposal, I expect to address those problems. After this proposal implemented, we have a faster and easier to use `opendal`.

# Guide-level explanation

In order to operate on object, we will use `Operator::object()` to create a new handler:

```rust
let o = op.object("path/to/file");
```

All operations that available for `Object` for now includes:

- `stat`: get object metadata (return error if not exist).
- `delete`: delete an object.
- `new_reader`: create a new reader to read data from this object.
- `new_writer`: create a new writer to write data into this object.

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
    let w = o.new_writer();
    let n = w
        .write_bytes("Hello, World!".to_string().into_bytes())
        .await?;
    assert_eq!(n, 13);

    // Read data from file;
    let mut r = o.new_reader();
    let mut buf = vec![];
    let n = r.read_to_end(&mut buf).await?;
    assert_eq!(n, 13);
    assert_eq!(String::from_utf8_lossy(&buf), "Hello, World!");

    // Get file's Metadata
    let meta = o.stat().await?;
    assert_eq!(meta.content_length(), 13);

    // Delete file.
    o.delete().await?;

    Ok(())
}
```

# Reference-level explanation

To support native `read` and `seek` operation on `fs`, we will split the `Read` operation into `sequential_read` and `random_read`:

```rust
async fn sequential_read(&self, args: &OpSequentialRead) -> Result<BoxedAsyncRead> {
    let _ = args;
    unimplemented!()
}
async fn random_read(&self, args: &OpRandomRead) -> Result<BoxedAsyncReadSeek> {
    let _ = args;
    unimplemented!()
}
```

- `sequential_read`: returns a `BoxedAsyncRead` which we can only do `read`.
- `random_read`: returns a `BoxedAsyncReadSeek` which we can do `read` and `seek` on it.

> `BoxedAsyncReadSeek` is just a boxed composition of `AsyncRead` and `AsyncSeek`.

For fs, `random_read` returns the underlying `File` object.
For object storage services like `s3`, `random_read` returns a `SeekableReader` wrapper.

Other changes are just a re-order of APIs.

# Drawbacks

None.

# Rationale and alternatives

TBD

# Prior art

## API design of beyondstorage/go-storage

TBD

# Unresolved questions

- What parts of the design do you expect to resolve through the RFC process before this gets merged?
- What related issues do you consider out of scope for this RFC that could be addressed in the future independently of the solution that comes out of this RFC?

# Future possibilities

- Implement `AsyncWrite` for `Writer` so that we can use `Writer` easier.