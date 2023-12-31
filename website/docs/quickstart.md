---
title: Quickstart
sidebar_position: 3
---

Apache OpenDAL™ can be easily integrated into different software with its Rust core and multilingual bindings.

## Rust core

OpenDAL's core is implemented in Rust programming language. The most convenient way to use OpenDAL in your Rust program add the OpenDAL Cargo crate as a dependency.

### Install

Run the following Cargo command in your project directory:

```shell
cargo add opendal
```

Or add the following line to your Cargo.toml:

```shell
opendal = "0.40.0"
```

### Demo

Try it out:

```rust
use opendal::Result;
use opendal::layers::LoggingLayer;
use opendal::services;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Pick a builder and configure it.
    let mut builder = services::S3::default();
    builder.bucket("test");

    // Init an operator
    let op = Operator::new(builder)?
        // Init with logging layer enabled.
        .layer(LoggingLayer::default())
        .finish();

    // Write data
    op.write("hello.txt", "Hello, World!").await?;

    // Read data
    let bs = op.read("hello.txt").await?;

    // Fetch metadata
    let meta = op.stat("hello.txt").await?;
    let mode = meta.mode();
    let length = meta.content_length();

    // Delete
    op.delete("hello.txt").await?;

    Ok(())
}
```

## Java binding

OpenDAL's Java binding is released to Maven central as [`org.apache.opendal:opendal-java:${version}`](https://central.sonatype.com/artifact/org.apache.opendal/opendal-java).

### Install

#### Maven

Generally, you can first add the `os-maven-plugin` for automatically detect the classifier based on your platform:

```xml
<build>
<extensions>
  <extension>
    <groupId>kr.motd.maven</groupId>
    <artifactId>os-maven-plugin</artifactId>
    <version>1.7.0</version>
  </extension>
</extensions>
</build>
```

Then add the dependency to opendal-java as following:

```xml
<dependencies>
<dependency>
  <groupId>org.apache.opendal</groupId>
  <artifactId>opendal-java</artifactId>
  <version>${opendal.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.opendal</groupId>
  <artifactId>opendal-java</artifactId>
  <version>${opendal.version}</version>
  <classifier>${os.detected.classifier}</classifier>
</dependency>
</dependencies>
```

#### Gradle

For Gradle, you can first add the `com.google.osdetector` for automatically detect the classifier based on your platform:

```groovy
plugins {
    id "com.google.osdetector" version "1.7.3"
}
```

Then add the dependency to opendal-java as following:

```groovy
dependencies {
    implementation "org.apache.opendal:opendal-java:$opendal.version"
    implementation "org.apache.opendal:opendal-java:$opendal.version:$osdetector.classifier"
}
```

#### Classified library

For details in specifying classified library, read the [dedicated explanation](https://github.com/apache/incubator-opendal/tree/main/bindings/java).

### Demo

Try it out:

```java
// Configure service
final Map<String, String> conf = new HashMap<>();
conf.put("root", "/tmp");
// Construct operator
final Operator op = Operator.of("fs", conf);
// Write data
op.write("hello.txt", "Hello, World!").join();
// Read data
final byte[] bs = op.read("hello.txt").join();
// Delete
op.delete("hello.txt").join();
```

## Python binding

OpenDAL's Python binding is released to PyPI repository as [`opendal`](https://pypi.org/project/opendal/).

### Install

Run the following command to install `opendal`:

```shell
pip install opendal
```

### Demo

Try it out:

```python
import opendal
import asyncio

async def main():
    op = opendal.AsyncOperator("fs", root="/tmp")
    await op.write("test.txt", b"Hello World")
    print(await op.read("test.txt"))

asyncio.run(main())
```

## Node.js binding

OpenDAL's Python binding is released to npm registry as [`opendal`](https://www.npmjs.com/package/opendal).

### Install

Run the following command to install `opendal`:

```shell
npm install opendal
```

### Demo

Try it out:

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
```
