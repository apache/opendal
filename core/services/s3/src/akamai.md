Builds an operator for Akamai Cloud Object Storage using the shared S3 implementation.

Set `endpoint` to the HTTPS S3 hostname for the bucket’s region and endpoint
type (E0, E1, E2, or E3). The endpoint selects where the data lives. The preset
signs in `us-east-1` by default and uses the virtual-hosted-style addressing
shown in Akamai’s s3cmd setup. Set `region` or `enable_virtual_host_style` to
accommodate a different configuration.

Use an Object Storage access key and its secret key. See Akamai’s
[endpoint list](https://techdocs.akamai.com/cloud-computing/docs/endpoint-types#supported-endpoint-types-by-region),
[AWS CLI setup](https://techdocs.akamai.com/cloud-computing/docs/using-the-aws-cli-with-object-storage),
and [s3cmd addressing example](https://techdocs.akamai.com/cloud-computing/docs/using-s3cmd-with-object-storage).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Akamai;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Akamai::default()
            .bucket("example-bucket")
            .endpoint("https://us-sea-9.linodeobjects.com"),
    )
}
```
