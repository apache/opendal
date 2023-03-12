# OpenDAL Node.js Binding

## Installation

### Node.js
```shell
npm install opendal
```

## Docs

see [index.d.ts](./index.d.ts)

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


## Test or Contributing

- Install latest `Rust`
- Install `Node.js@10+` which fully supported `Node-API`

We are using `corepack` to specific package manager:
```shell
corepack enable
```
`corepack` is distributed with Node.js, so you do not need to specifically look for a way to install it.

# Build Node.js bindings

```bash
yarn
yarn build
yarn test
```


## License
[Apache v2.0](https://www.apache.org/licenses/LICENSE-2.0)
