# Upgrade to v0.44

## Breaking change

### Services

Because of [a TLS lib issue](https://github.com/apache/opendal/issues/3650), we temporarily disable the `services-ftp` feature.

### Public API

Now, the `list` operation returns `Array<Entry>` instead of a lister.
Also, we removed `scan`, you can use `list('some/path', {recursive: true})`/`listSync('some/path', {recursive: true})` instead of `scan('some/path')`/`scanSync('some/path')`.
