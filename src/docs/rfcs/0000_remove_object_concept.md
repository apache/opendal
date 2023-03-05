- Proposal Name: `remove_object_concept`
- Start Date: `2023-03-05`
- RFC PR: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/pull/0000)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Remove the Object concept to make OpenDAL easier to understand.

# Motivation

OpenDAL introduces [Object Native API][super::super::0041_object_native_api] to resolve the problem of not easy to use:

```diff
- let reader = SeekableReader::new(op, path, stream_len);
+ let reader = op.object(&path).reader().await?;

- op.stat(&path).run().await
+ op.object(&path).stat().await
```

However, times changing. Aftre `list` operation has also been moved to `Object` level, `Object` is more like a wrapper of `Opeator`. And the only meaning API of `Opeator` is `Opeator::object`.

Thare two problems:

## Extra cost

`Object::new()` is not zero cost:

```rust
pub(crate) fn with(op: Operator, path: &str, meta: Option<ObjectMetadata>) -> Self {
    Self {
        acc: op.inner(),
        path: Arc::new(normalize_path(path)),
        meta: meta.map(Arc::new),
    }
}
```

`Object` needs to hold an `Operator` and an `Arc<String>`. After we introduced [Query Based Metadata][super::super::1398_query_based_metadata], we don't need to do operations over object any more.

## Complex concepts

`Object` is used so widely in different areas, as we can't explain what `opendal::Object` is in a short sentences. Even worse, `Object` could confuse our users to think `opendal::Object` is designed for object storage services.

So I propose to remove the intermidia API layer of `Object` and allowing users to use `Operator` directly.

# Guide-level explanation

After this RFC implemented, our users can:

```rust
# read all content of file
op.read("file").await?;
# read part content of file
op.range_read("file", 0..1024).await?;
# create a reader
op.reader("file").await?;

# write all content into file
op.write("file", bs).await?;
# create a writer
op.wirter("file").await?;

# get metadata of a path
op.stat("path").await?;

# delete a path
op.delete("path").await?;
# remove paths
op.remove(vec!["path_a"]).await?;
# remove path recursively
op.remove_all("path").await?;

# create a dir
op.create_dir("dir/").await?;

# list a dir
op.list("dir/").await?;
# scan a dir
op.scan("dir/").await?;
```

And we will add `BlockingOperator` to make blocking operations easier to use:

```rust
# this is a cheap call without allocation
let bop = op.blocking();

# read all content
bop.read("file")?;
# write all content
bop.write("file", bs)?;
```

# Reference-level explanation

TODO

# Drawbacks

TODO

# Rationale and alternatives

TODO

# Prior art

TODO

# Unresolved questions

TODO

# Future possibilities

TODO
