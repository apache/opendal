# Upgrade to v0.59

## Upgrade `object_store` to v0.14

`object_store_opendal` 0.59 implements the `ObjectStore` trait from `object_store` 0.14. Update direct dependencies together so they use the same trait version:

```diff
-object_store = "0.13"
-object_store_opendal = "0.58"
+object_store = "0.14.1"
+object_store_opendal = "0.59"
```

Projects that require `object_store` 0.13, including projects that use DataFusion 54, should pin `object_store_opendal` to `=0.58.0`.
