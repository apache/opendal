# Upgrade to v0.44

## Breaking change

Because of [a TLS lib issue](https://github.com/apache/opendal/issues/3650), we temporarily disable the `services-ftp` feature.


# Upgrade to v0.42

## Breaking change for layers

Operator and BlockingOperator won't accept `layers` anymore. Instead, we provide a `layer` API:

```python
op = opendal.Operator("memory").layer(opendal.layers.RetryLayer())
```

We removed not used layers `ConcurrentLimitLayer` and `ImmutableIndexLayer` along with this change.

## File and AsyncFile

OpenDAL removes `Reader` and `AsyncReader` classes, instead, we provide file-like object `File` and `AsyncFile`.

Open a file for reading in blocking way:

```python
with op.open(filename, "rb") as r:
    content = r.read()
```

Open a file for reading in async way:

```python
async with await op.open(filename, "rb") as r:
    content = await r.read()
```

## Breaking change for Errors

We remove the old error classes and provide a couple of Exception based class for the error handling.

1. `opendal.Error` is based class for all the exceptions now.
2. `opendal.exceptions.Unexpected` is added.
3. `opendal.exceptions.Unsupported` is added.
4. `opendal.exceptions.ConfigInvalid` is added.
5. `opendal.exceptions.NotFound` is added.
6. `opendal.exceptions.PermissionDenied` is added.
7. `opendal.exceptions.IsADirectory` is added.
8. `opendal.exceptions.NotADirectory` is added.
9. `opendal.exceptions.AlreadyExists` is added.
10. `opendal.exceptions.IsSameFile` is added.
11. `opendal.exceptions.ConditionNotMatch` is added.
12. `opendal.exceptions.ContentTruncated` is added.
13. `opendal.exceptions.ContentIncomplete` is added.
14. `opendal.exceptions.InvalidInput` is added.
