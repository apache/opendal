Builds an operator for a Garage deployment using the shared S3 implementation.

Set `endpoint` to the cluster’s S3 API URL and `region` to `s3_region` from
its `[s3_api]` configuration. Garage checks the signing region. The preset
uses path-style addressing by default. Enable virtual-hosted-style addressing
when the cluster has a matching `root_domain` and the required DNS and TLS
configuration.

Use a Garage access key and secret key with access to the bucket. See the
[configuration reference](https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#s3_api)
and [S3 client setup](https://garagehq.deuxfleurs.fr/documentation/connect/cli/).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

Garage does not support object versioning or restore. See the
[Garage S3 compatibility table](https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/)
for the available operations.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Garage;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Garage::default()
            .bucket("example-bucket")
            .endpoint("http://127.0.0.1:3900")
            .region("garage"),
    )
}
```
