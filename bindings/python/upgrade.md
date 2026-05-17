# Upgrade to v0.47

## New feature: HttpClientLayer for custom HTTP client configuration

OpenDAL Python bindings now support `HttpClientLayer`, allowing you to customize the HTTP client used for all operations. This is particularly useful for:

- **Testing with self-signed certificates**: Use `danger_accept_invalid_certs=True` when connecting to local/development services with self-signed SSL certificates (e.g., local MinIO, S3-compatible services)
- **Custom timeouts**: Set request-specific timeout values
- **Advanced HTTP configurations**: More options may be added in future versions

### Example: Accept invalid SSL certificates (testing only)

```python
import opendal
from opendal.layers import HttpClientLayer

# Create a custom HTTP client that accepts invalid certificates
# WARNING: Only use this in testing/development environments!
client = opendal.HttpClient(danger_accept_invalid_certs=True)

# Create the layer
http_layer = HttpClientLayer(client)

# Apply to your operator
op = opendal.Operator(
    "s3",
    bucket="my-bucket",
    endpoint="https://localhost:9000",
    access_key_id="minioadmin",
    secret_access_key="minioadmin"
).layer(http_layer)

# Now you can use the operator normally
op.write("test.txt", b"Hello World")
```

### Example: Custom timeout

```python
import opendal
from opendal.layers import HttpClientLayer

# Create HTTP client with 30 second timeout
client = opendal.HttpClient(timeout=30.0)
http_layer = HttpClientLayer(client)

op = opendal.Operator("s3", bucket="my-bucket").layer(http_layer)
```

**Security Warning**: `danger_accept_invalid_certs=True` disables SSL/TLS certificate verification. Never use this in production environments.

## Breaking change: Module exports are explicit

`opendal.__init__` now only re-exports the `capability`, `exceptions`, `file`, `layers`, `services`, `types`, `Operator`, and `AsyncOperator` symbols. Imports such as:

```python
from opendal import Metadata, Layer
```

no longer work. Update them to use the dedicated submodules:

```python
from opendal.types import Metadata
from opendal.layers import Layer
```

The legacy helper module `opendal.__base` has also been removed together with `_Base`.

## Breaking change: Capability accessors renamed

Both `Operator.full_capability()` and `AsyncOperator.full_capability()` have been renamed to `capability()`. Adjust your code accordingly:

```diff
-caps = op.full_capability()
+caps = op.capability()
```

## Breaking change: Service identifiers now have typed enums

The constructors for `Operator` / `AsyncOperator` provide overloads that accept `opendal.services.Scheme` members. While plain strings are still accepted at runtime, type checkers (pyright/mypy) expect the new enum values. Migrate code bases that relied on importing the old `Scheme` enum from `opendal` to `from opendal import services` and use `services.Scheme.<NAME>`.

# Upgrade to v0.46

## Breaking change: Native blocking API removed

OpenDAL has removed the native blocking API in the core. The Python binding's blocking API now uses an async runtime internally. This is a transparent change and should not affect most users, but:

- The blocking API now requires an async runtime to be available
- Performance characteristics may be slightly different
- All blocking operations are now implemented as async operations running in a tokio runtime

## Breaking change: Removed services

The following services have been removed due to lack of maintainers and users:

- `atomicserver` - This service is no longer supported
- `icloud` - This service is no longer supported
- `nebula-graph` - This service is no longer supported

If you were using any of these services, you'll need to migrate to an alternative storage backend.

## Breaking change: Chainsafe service removed

The Chainsafe service has been sunset and is no longer available.

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

# With read options
with op.open(filename, "rb", offset=1024, size=2048) as r:
    content = r.read()
```

Open a file for reading in async way:

```python
async with await op.open(filename, "rb") as r:
    content = await r.read()

# With read options
async with await op.open(filename, "rb", offset=1024, size=2048) as r:
    content = await r.read()
```

Open a file for writing:

```python
# Blocking write
with op.open(filename, "wb") as w:
    w.write(b"hello world")

# With write options
with op.open(filename, "wb", content_type="text/plain", if_not_exists=True) as w:
    w.write(b"hello world")

# Async write
async with await op.open(filename, "wb") as w:
    await w.write(b"hello world")
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
