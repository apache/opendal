A distributed file system that provides high-throughput access to application data.
Using [Native Rust HDFS client](https://github.com/Kimahriman/hdfs-native).

## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] write (if_not_exists)
- [x] delete
- [x] list
- [ ] copy
- [x] rename
- [x] rename (if_not_exists)
- [ ] ~~presign~~

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

HDFS Native deletes a directory and its descendants in a single NameNode
request when `recursive(true)` is set. Deleting a non-empty directory without
this option returns an error. Deleting `/` targets the configured root. If the
configured root is the HDFS filesystem root, HDFS keeps it intact.

## Configuration

Use [`crate::HdfsNativeConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Encryption zones

Reading and writing files in HDFS encryption zones requires a Hadoop KMS client.
Enable one of these features on `opendal-service-hdfs-native`. Applications that
use the `opendal` facade add it as a direct dependency so Cargo unifies the
features:

| Feature | KMS transport |
| --- | --- |
| `kms` | `http://` only |
| `kms-rustls-tls` | Rustls with its default crypto provider |
| `kms-rustls-no-provider` | Rustls with the crypto provider the application installs |
| `kms-native-tls` | Platform TLS |

```shell
cargo add opendal --features services-hdfs-native
cargo add opendal-service-hdfs-native --features kms-rustls-tls
```

The KMS endpoint comes from `hadoop.security.key.provider.path`, set in
`core-site.xml` or in `options`.
