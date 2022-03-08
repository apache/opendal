- Proposal Name: `path-normalization`
- Start Date: 2022-03-08
- RFC PR: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/pull/0000)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Implement path normalization to enhance user experience.

# Motivation

OpenDAL's current path behavior makes users confused:

- [operator.object("/admin/data/") error](https://github.com/datafuselabs/opendal/issues/107)
- [Read /admin/data//ontime_200.csv return empty](https://github.com/datafuselabs/opendal/issues/109)

They are different bugs that reflect the same root cause: path is not well normalized.

On local fs, we can read the same path with different path: `abc/def/../def`, `abc/def`, `abc//def`, `abc/./def`.

There no magic here: our stdlib do the dirty job. For example:

- [std::path::PathBuf::canonicalize](https://doc.rust-lang.org/std/path/struct.PathBuf.html#method.canonicalize): Returns the canonical, absolute form of the path with all intermediate components normalized and symbolic links resolved.
- [std::path::PathBuf::components](https://doc.rust-lang.org/std/path/struct.PathBuf.html#method.components): Produces an iterator over the Components of the path. When parsing the path, there is a small amount of normalization...

But for s3 alike storage system, there's no such helpers: `abc/def/../def`, `abc/def`, `abc//def`, `abc/./def` refers entirely different objects. So users may confuse why I can't get object with this path.

So OpenDAL need to implement path normalization to enhance the user experience.

# Guide-level explanation

We will do path normalization automatically.

The following rules will be applied (so far):

- Remove `//` inside path: `op.object("abc/def")` and `op.object("abc//def")` will resolve to the same object.
- Make sure path under `root`: `op.object("/abc")` and `op.object("abc")` will resolve to the same object.

Other rules still need more consideration, so we will leave them for the future.

For s3, `abc//def` is different from `abc/def` indeed. To make it possible to access not normalized path, we will provide a new flag for builder:

```rust
let builder = Backend::build().disable_path_normalization()
```

In this way, user can control the path more precisely.

# Reference-level explanation

We will build the real path via `{root}/{path}` and replace all `//` into `/` instead.

# Drawbacks

None

# Rationale and alternatives

## How about link?

If we build real path via `{root}/{path}`, the link object may be inaccessible.

No good ideas so far, maybe we can add a new flag to control the link behavior. For now, there's no feature request for link support.

Let's leave for the future to resolve.

# Prior art

None

# Unresolved questions

None

# Future possibilities

None
