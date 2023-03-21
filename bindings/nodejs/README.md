# OpenDAL Node.js Binding

## Installation

### Node.js

```shell
npm install opendal
```

## Docs

See our documentations on [opendal.apache.org](https://opendal.apache.org/docs/nodejs/).

## Usage

```javascript
import { Operator } from "opendal";

async function main() {
  const op = new Operator("fs", { root: "/tmp" });
  await op.write("test", "Hello, World!");
  const bs = await op.read("test");
  console.log(new TextDecoder().decode(bs));
  const meta = await op.stat("test");
  console.log(`contentLength: ${meta.contentLength}`);
}

main();
```

## Services

- [azblob](https://docs.rs/opendal/latest/opendal/services/struct.Azblob.html): [Azure Storage Blob](https://azure.microsoft.com/en-us/services/storage/blobs/) services.
- [dashmap](https://docs.rs/opendal/latest/opendal/services/struct.Dashmap.html): [dashmap](https://github.com/xacrimon/dashmap) backend support.
- [fs](https://docs.rs/opendal/latest/opendal/services/struct.Fs.html): POSIX alike file system.
- [gcs](https://docs.rs/opendal/latest/opendal/services/struct.Gcs.html): [Google Cloud Storage](https://cloud.google.com/storage) Service.
- [ghac](https://docs.rs/opendal/latest/opendal/services/struct.Ghac.html): [Github Action Cache](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows) Service.
- [http](https://docs.rs/opendal/latest/opendal/services/struct.Http.html): HTTP read-only services.
- [ipmfs](https://docs.rs/opendal/latest/opendal/services/struct.Ipmfs.html): [InterPlanetary File System](https://ipfs.tech/) MFS API support.
- [memory](https://docs.rs/opendal/latest/opendal/services/struct.Memory.html): In memory backend.
- [obs](https://docs.rs/opendal/latest/opendal/services/struct.Obs.html): [Huawei Cloud Object Storage](https://www.huaweicloud.com/intl/en-us/product/obs.html) Service (OBS).
- [oss](https://docs.rs/opendal/latest/opendal/services/struct.Oss.html): [Aliyun Object Storage Service](https://www.aliyun.com/product/oss) (OSS).
- [s3](https://docs.rs/opendal/latest/opendal/services/struct.S3.html): [AWS S3](https://aws.amazon.com/s3/) alike services.
- [webdav](https://docs.rs/opendal/latest/opendal/services/struct.Webdav.html): [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) Service Support.
- [webhdfs](https://docs.rs/opendal/latest/opendal/services/struct.Webhdfs.html): [WebHDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) Service Support.

## Contributing

- Install latest `Rust`
- Install `Node.js@10+` which fully supported `Node-API`

We are using `corepack` to specific package manager:

```shell
corepack enable
```

`corepack` is distributed with Node.js, so you do not need to specifically look for a way to install it.


### Build

```bash
# Install dependencies.
yarn
# Build from source.
yarn build
# Build from source with debug info.
yarn build:debug
```

### Test

```bash
yarn test
```

We use [`Cucumber`](https://cucumber.io/) for behavior testing. Refer to [here](https://cucumber.io/docs/guides/overview/) for more information about `Cucumber`.



## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

