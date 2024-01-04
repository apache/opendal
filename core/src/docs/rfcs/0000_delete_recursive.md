- Proposal Name: `deleter_api`
- Start Date: 2024-01-03
- RFC PR: [apache/incubator-opendal#0000](https://github.com/apache/incubator-opendal/pull/0000)
- Tracking Issue: [apache/incubator-opendal#0000](https://github.com/apache/incubator-opendal/issues/0000)

# Summary


# Motivation


# Guide-level explanation

```rust
// Init a deleter to start batch delete tasks.
let deleter = op.deleter().await?;
// List all files that ends with tmp
let lister = op.lister(path).await?
  .filter(|x|future::ready(x.ends_with(".tmp")));

// Forward all paths into deleter.
lister.forward(deleter).await?;
```

# Reference-level explanation

# Drawbacks


# Rationale and alternatives


# Prior art



# Unresolved questions



# Future possibilities

