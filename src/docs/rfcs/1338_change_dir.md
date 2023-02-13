- Proposal Name: Change Directory Layer
- Start Date: 2023-02-10
- RFC PR: [datafuselabs/opendal#1338](https://github.com/datafuselabs/opendal/pull/1338)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Supporting a change directory layer, workings like the `cd` command in shell, but much stronger.

# Motivation

1. Offering a more filesystem-like and safe interface for users.
2. Some table formats, like `Iceberg`, using URL like `s3://bucket/path/to/file` instead of relative pathes to point to files, requiring a stronger way to access.

# Guide-level explanation

Using a change directory layer allows user to get a new directory changed operator easily. For example, `cd` to a subdirectory like `home`:

```rust
let op: Operator = parent_op.layer(ChangeDirLayer::new("home"));
```

Users can also changing directories using absolute pathes:

```rust
let op: Operator = parent_op.layer(ChangeDirLayer::new("/path/to/working_dir/home"));
```

Users can even use URLs to the directory:

```rust
let op: Operator = parent_op.layer(ChangeDirLayer::url("s3://bucket/path/to/working_dir/home"));
```

The Operators users get will have a changed working directory:

```rust
// open /path/to/working_dir/home/path/to/file
let oh: ObjectHandler = op.object("path/to/file").open().await?;
```

## Limitations

For safety, the `ChangeDirLayer` only supports navigating within the parent's working directory.

```rust
// panic with debug assert
// double dot is not supported
let op: Operator = parent_op.layer(ChangeDirLayer::new(".."));
// panic with debug assert
// navigate out of current working directory is not allowed
let op: Operator = parnet_op.layer(ChangeDirLayer::new("/path/not/to/working_dir/home"));
// panic with debug assert
// navigating outside of current service is not allowed
let op: Operator = parent_op.layer(ChangeDirLayer::url("http://malicious.example.com/steal/your/secret"));
```

# Reference-level explanation

This RFC suggests a new layer called `ChangeDirLayer`.

```rust
struct ChangeDirLayer {
    // used for building phase
    scheme: Option<String>,
    domain: Option<String>,
    path: Option<String>,

    // used for applying phase
    sub_dir: OnceCell<String>,
}
```

For `ChangeDirLayer::new(&str)`, if a path not beginning with '/' is given, the `path` will be set; else the `sub_dir` will be set.
For `ChangeDirLayer::url(&str)`, the url will be resolve to `<scheme>://<domain>/<path>`, these 3 members will be set.

When applying, the underlayed operator will check whether `sub_dir` exists. If exists, operator will directly pre-concatenate it to the path.
If not, the operator should check if `scheme`, `domain` matches itself, and `path` belongs to its root. Corresponding data could be found in `AccessorMetadata`.
Then the relative path resolved will be set to `sub_dir`. This could be done in a `OnceCell`.

# Drawbacks

Concatenating path draws performance.

# Rationale and alternatives

## SubdirLayer

The project once had a layer named `SubdirLayer`, but this layer only offers building from pathes, building from URLs is not supported.

## Building a new operator

Some projects has its own wrapped operator types and implemented root change abilities, but building from URLs is not supported.

# Prior art

None.

# Unresolved questions

None.

# Future possibilities

None.
