# Unreleased

## Breaking change for layers

Operator and BlockingOperator won't accept `layers` anymore. Instead, we provide a `layer` API:

```python
op = opendal.Operator("memory").layer(opendal.layers.RetryLayer())
```

We removed not used layers `ConcurrentLimitLayer` and `ImmutableIndexLayer` along with this change.
