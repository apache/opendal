- Proposal Name: `remove_object_concept`
- Start Date: `2023-03-05`
- RFC PR: [datafuselabs/opendal#1477](https://github.com/datafuselabs/opendal/pull/1477)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Eliminating the Object concept to enhance the readability of OpenDAL.

# Motivation

OpenDAL introduces [Object Native API][crate::docs::rfcs::rfc_0041_object_native_api] to resolve the problem of not being easy to use:

```diff
- let reader = SeekableReader::new(op, path, stream_len);
+ let reader = op.object(&path).reader().await?;

- op.stat(&path).run().await
+ op.object(&path).stat().await
```

However, times are changing. After the list operation has been moved to the `Object` level, `Object` is now more like a wrapper for `Operator`. The only meaningful API of `Operator` now is `Operator::object`.

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

The `Object` must contain an `Operator` and an `Arc` of strings. With the introduction of [Query Based Metadata][crate::docs::rfcs::rfc_1398_query_based_metadata], there is no longer a need to perform operations on the object.

## Complex concepts

The term `Object` is applied in various fields making it difficult to provide a concise definition of `opendal::Object`. Moreover, this could potentially confuse our users who may assume that `opendal::Object` is primarily intended for object storage services.

I propose eliminating the intermediate API layer of `Object` and enabling users to directly utilize `Operator`.

# Guide-level explanation

After this RFC is implemented, our users can:

```rust
# read all content of the file
op.read("file").await?;
# read part content of the file
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

We will include the `BlockingOperator` for enhanced ease of use while performing blocking operations.

```rust
# this is a cheap call without allocation
let bop = op.blocking();

# read all content
bop.read("file")?;
# write all content
bop.write("file", bs)?;
```

# Reference-level explanation

We will remove `Object` entirely and move all `Object` APIs to `Operator` instead:

```rust
- op.object("path").read().await
+ op.read("path").await
```

Along with this change, we should also rename the `ObjectXxx` structs like `ObjectReader` to `Reader`.

# Drawbacks

## Breaking Changes

This RFC proposes a major breaking change that will require almost all current usage to be rewritten.

# Rationale and alternatives

None

# Prior art

None

# Unresolved questions

None

# Future possibilities

None
