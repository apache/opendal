# OpenDAL Node.js examples

## s3 example

```javascript
import { Operator } from "opendal"

async function main() {
  // you can try to config Operator in the following two ways
  // 1. config Operator plainly
  const op = new Operator(
    "s3", {
    root: "/test_opendal",
    bucket: "your bucket name",
    region: "your bucket region",
    endpoint: "your endpoint",
    access_key_id: "your access key id",
    secret_access_key: "your secret access key",
  })

  // 2. Or config Operator from .env, opendal will load from the environment by default
  const endpoint = process.env.AWS_S3_ENDPOINT
  const region = process.env.AWS_S3_REGION
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID
  const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY
  const bucket = process.env.AWS_BUCKET

  const opendal = new Operator('s3', {
    root: '/',
    bucket,
    endpoint,
  })

  // Use operator to do some operations
  await op.write("test", "Hello, World!")
  const bs = await op.read("test")
  console.log(new TextDecoder().decode(bs))
  const meta = await op.stat("test")
  console.log(`contentLength: ${meta.contentLength}`)
}

main()
```
