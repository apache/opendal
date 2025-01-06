# Apache OpenDAL™ Java Bindings

![](https://img.shields.io/badge/status-released-blue)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.opendal/opendal.svg?logo=Apache+Maven&logoColor=blue)](https://central.sonatype.com/search?q=opendal&smo=true)
[![Website](https://img.shields.io/badge/opendal-OpenDAL_Website-red?logo=Apache&logoColor=red)](https://opendal.apache.org/docs/java/)

![](https://github.com/apache/opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

## Example

```java
import java.util.HashMap;
import java.util.Map;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;

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

## Getting Started

This project is built upon the native OpenDAL lib. And it is released for multiple platforms that you can use a classifier to specify the platform you are building the application on.

### Maven

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

Then add the dependency to `opendal` as following:

```xml
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

For Gradle, you can first add the `com.google.osdetector` for automatically detect the classifier based on your platform:

```groovy
plugins {
    id "com.google.osdetector" version "1.7.3"
}
```

Then add the dependency to `opendal as following:

```groovy
dependencies {
    implementation "org.apache.opendal:opendal:$opendalVersion"
    implementation "org.apache.opendal:opendal:$opendalVersion:$osdetector.classifier"
}
```

### Classified library

Note that the dependency without classifier ships all classes and resources except the "opendal_java" shared library. And those with classifier bundle only the shared library.

For downstream usage, it's recommended:

* Depend on the one without classifier to write code; 
* Depend on the classified ones with "test" for testing.

To load the shared library correctly, you can choose one of the following approaches:

* Append the classified JARs to the classpath at the runtime;
* Depend on the classified JARs and build a fat JAR (You may need to depend on all the provided classified JARs for running on multiple platforms);
* Build your own "opendal_java" shared library and specify "-Djava.library.path" to the folder containing that shared library.

## Build

This project provides OpenDAL Java bindings with artifact name `opendal`. It depends on JDK 8 or later.

You can use Maven to build both Rust dynamic lib and JAR files with one command now:

```shell
./mvnw clean package -DskipTests=true
```

## Run tests

Currently, all tests are written in Java.

You can run the base tests with the following command:

```shell
./mvnw clean verify
```

## Code style

This project uses [spotless](https://github.com/diffplug/spotless) for code formatting so that all developers share a consistent code style without bikeshedding on it.

You can apply the code style with the following command::

```shell
./mvnw spotless:apply
```

## Run behavior tests

Services behavior tests read necessary configs from env vars or the `.env` file.

You can copy [.env.example](/.env.example) to `${project.rootdir}/.env` and change the values on need, or directly set env vars with `export KEY=VALUE`.

Take `fs` for example, we need to enable bench on `fs` on `/tmp`:

```properties
OPENDAL_TEST=fs
OPENDAL_FS_ROOT=/tmp
```

You can run service behavior tests of enabled with the following command:

```shell
./mvnw test -Dtest="behavior.*Test"
```

Remember to enable the necessary features via `-Dcargo-build.features=services-xxx` when running specific service test:

```shell
export OPENDAL_TEST=redis
export OPENDAL_REDIS_ENDPOINT=tcp://127.0.0.1:6379
export OPENDAL_REDIS_ROOT=/
export OPENDAL_REDIS_DB=0
./mvnw test -Dtest="behavior.*Test" -Dcargo-build.features=services-redis
```

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
