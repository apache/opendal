# Apache OpenDAL™ Java Bindings

[![](https://img.shields.io/badge/status-released-blue)](https://central.sonatype.com/search?q=opendal&smo=true)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.opendal/opendal.svg?logo=Apache+Maven&logoColor=blue)](https://central.sonatype.com/search?q=opendal&smo=true)
[![Website](https://img.shields.io/badge/opendal-OpenDAL_Website-red?logo=Apache&logoColor=red)](https://opendal.apache.org/docs/java/)

A native Java binding for Apache OpenDAL™: access S3, GCS, Azure Blob, HDFS, the
local filesystem, and many more services through one API.

> **Note**: This binding has its own independent version number, which may differ from the Rust core version. When checking for updates or compatibility, always refer to this binding's version rather than the core version.

## Useful Links

- **User guide**: [opendal.apache.org/docs/bindings/java](https://opendal.apache.org/docs/bindings/java) — install, connect, common tasks, and going to production.
- **API reference**: [opendal.apache.org/docs/java](https://opendal.apache.org/docs/java/)
- **Services & configuration**: [opendal.apache.org/services](https://opendal.apache.org/services)
- **Upgrade guide**: [`upgrade.md`](./upgrade.md)

## Installation

The binding ships a platform-specific native library, so you depend on the main
artifact plus a classifier for your platform. An OS detector plugin fills in the
classifier automatically.

### Maven

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

<dependencies>
  <dependency>
    <groupId>org.apache.opendal</groupId>
    <artifactId>opendal</artifactId>
    <version>${opendal.version}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.opendal</groupId>
    <artifactId>opendal</artifactId>
    <version>${opendal.version}</version>
    <classifier>${os.detected.classifier}</classifier>
  </dependency>
</dependencies>
```

### Gradle

```groovy
plugins {
    id "com.google.osdetector" version "1.7.3"
}

dependencies {
    implementation "org.apache.opendal:opendal:$opendalVersion"
    implementation "org.apache.opendal:opendal:$opendalVersion:$osdetector.classifier"
}
```

On musl-based Linux distributions such as Alpine, use the `linux-x86_64-musl` or
`linux-aarch_64-musl` classifier instead of the detected one.

## Quickstart

```java
import java.util.HashMap;
import java.util.Map;
import org.apache.opendal.AsyncOperator;

public class Main {
  public static void main(String[] args) {
    final Map<String, String> conf = new HashMap<>();
    conf.put("root", "/tmp");

    try (AsyncOperator op = AsyncOperator.of("fs", conf)) {
      op.write("/path/to/data", "Hello world").join();
      System.out.println(new String(op.read("/path/to/data").join()));
    }
  }
}
```

Use the synchronous `Operator` for blocking calls, or `AsyncOperator` for
`CompletableFuture`-based calls.

## Documentation

The full user guide — getting started, connecting to services, common tasks, and
going to production — lives at
[opendal.apache.org/docs/bindings/java](https://opendal.apache.org/docs/bindings/java).

## Contributing

This project is built upon the native OpenDAL library and depends on JDK 8 or
later. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build the binding, run
tests, and apply the code style.

## Used by

Check out the [users](./users.md) list for more details on who is using OpenDAL.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
</content>
