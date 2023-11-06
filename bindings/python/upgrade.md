# Unreleased

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
