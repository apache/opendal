Builds an operator for the Storj-hosted S3 Gateway using the shared S3 implementation.

Set `endpoint` to the gateway URL Storj issues with the S3 access key and
secret key for the access grant. For example, Storj’s global endpoint is
`https://gateway.storjshare.io`. The preset uses path-style addressing and
signs in `us-east-1` by default. Override `region` when the client setup calls
for a different signing scope. A bucket’s placement tier is selected separately
from the signing region.

Use the generated S3 key pair. See the [hosted gateway guide](https://storj.dev/dcs/api/s3/s3-compatible-gateway),
[Node.js addressing example](https://storj.dev/dcs/code/aws/nodejs),
[Go signing example](https://storj.dev/dcs/code/presigned-urls-serverless-cloud),
and [S3 compatibility reference](https://storj.dev/dcs/api/s3/s3-compatibility).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

Storj’s [listing semantics](https://storj.dev/dcs/api/s3/s3-compatibility)
differ from AWS S3. In particular, the gateway’s listing order need not match
the lexical order of the unencrypted keys.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Storj;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Storj::default()
            .bucket("example-bucket")
            .endpoint("https://gateway.storjshare.io"),
    )
}
```
