- Proposal Name: `list_with_metakey`
- Start Date: 2023-08-04
- RFC PR: [apache/incubator-opendal#2779](https://github.com/apache/incubator-opendal/pull/2779)
- Tracking Issue: [apache/incubator-opendal#0000](https://github.com/apache/incubator-opendal/issues/0000)

# Summary

Move `metadata` API to `Lister` to simplify the usage.

# Motivation

The current `Entry` metadata API is:

```rust
use opendal::Entry;
use opendal::Metakey;

let meta = op
    .metadata(&entry, Metakey::ContentLength | Metakey::ContentType)
    .await?;
```

This API is difficult to understand and rarely used correctly. And in reality, users always fetch the same set of metadata during listing.

Take one of our users code as an example:

```rust
let stream = self
    .inner
    .scan(&path)
    .await
    .map_err(|err| format_object_store_error(err, &path))?;

let stream = stream.then(|res| async {
    let entry = res.map_err(|err| format_object_store_error(err, ""))?;
    let meta = self
        .inner
        .metadata(&entry, Metakey::ContentLength | Metakey::LastModified)
        .await
        .map_err(|err| format_object_store_error(err, entry.path()))?;

    Ok(format_object_meta(entry.path(), &meta))
});

Ok(stream.boxed())
```

By moving metadata to `lister`, our user code can be simplified to:

```rust
let stream = self
    .inner
    .scan_with(&path)
    .metakey(Metakey::ContentLength | Metakey::LastModified)
    .await
    .map_err(|err| format_object_store_error(err, &path))?;

let stream = stream.then(|res| async {
    let entry = res.map_err(|err| format_object_store_error(err, ""))?;
    let meta = entry.into_metadata()

    Ok(format_object_meta(entry.path(), &meta))
});

Ok(stream.boxed())
```

By introducing this change:

- Users don't need to capture `Operator` in the closure.
- Users don't need to do async call like `metadata()` again.

# Guide-level explanation

The new API will be:

```rust
let entries: Vec<Entry> = op
  .list_with("dir")
  .metakey(Metakey::ContentLength | Metakey::ContentType).await?;

let meta: &Metadata = entries[0].metadata();
```

Metadata can be queried directly when listing entries via `metadata()`, and later extracted via `into_parts()`.

# Reference-level explanation

We will add `metakey` into `OpList`. Underlying services can use those information to try their best to fetch the metadata.

There are following possibilities:

- The entry metadata is met: `Lister` return the entry directly
- The entry metadata is not met and not fully filled: `Lister` will try to send `stat` call to fetch the metadata
- The entry metadata is not met and fully filled: `Lister` will return the entry directly.

To make sure we can handle all metadata correctly, we will add a new capability called `stat_max_metakey`. This capability will be used to indicate the maximum number of metadata that can be fetched via `stat` call. `Lister` can use this capability to decide whether to send `stat` call or not.

Services' lister implementation should not changed.

# Drawbacks

None

# Rationale and alternatives

Keeping the complex standalone API has limited benefit given low usage.

# Prior art

None

# Unresolved questions

None

# Future possibilities

## Add glob and regex support for Lister

We can add `glob` and `regex` support for `Lister` to make it more powerful.
