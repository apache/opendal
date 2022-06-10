## Remove a dir recursively

OpenDAL has native support for remove a dir recursively via [`BatchOperator`](/opendal/struct.BatchOperator.html).

```rust
op.batch().remove_all("path/to/dir").await?
```

**Use this function in cautions to avoid unexpected data loss.**
