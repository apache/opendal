- Proposal Name: `create-dir`
- Start Date: 2022-03-23
- RFC PR: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/pull/0000)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Add creating dir support for OpenDAL.

# Motivation

Interoperability between OpenDAL services requires dir support. Object storage system will simulate dir operations via object endswith `/`. But we can't share the same behavior with `fs`, as `mkdir` is a separate syscall.

So we need to unify the behavior about dir across different services.

# Guide-level explanation

After this proposal got merged, we will treat all path that end with `/` as a dir.

For example:

- `read("abc/")` will return an `IsDir` error.
- `write("abc/")` will return an `IsDir` error.
- `stat("abc/")` will be guaranteed to return a dir or a `NotDir` error.
- `delete("abc/")` will be guaranteed to delete a dir or `NotDir` / `NotEmpty` error.
- `list("abc/")` will be guaranteed to list a dir or a `NotDir` error.

And we will support create an empty object:

```rust
// create a dir object "abc/", we will allow user ignore the ending "/"
let _ = op.object("abc").create_dir().await?;
// create a dir object "abc/"
let _ = op.object("abc/").create_dir().await?;
// create a file object "abc"
let _ = op.object("abc").create_file().await?;
// returns an error that `IsDir`.
let _ = op.object("abc/").create_file().await?; 
```

# Reference-level explanation

And we will add a new API called `create` to create an empty object.

```rust
struct OpCreate {
    path: String,
    mode: ObjectMode,
}

pub trait Accessor: Send + Sync + Debug {
    async fn create(&self, args: &OpCreate) -> Result<Metadata>;
}
```

`Object` will expose API like `create_dir` and `create_file` which will call `Accessor::create()` internally.

# Drawbacks

None

# Rationale and alternatives

None

# Prior art

None

# Unresolved questions

None

# Future possibilities

None