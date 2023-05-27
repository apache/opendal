# OpenDAL Java Bindings

## Usage

You can use the package by adding the dependency as following:

```xml
<dependencies>
  <dependency>
    <groupId>org.apache.opendal</groupId>
    <artifactId>opendal-java</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <classifier>${os.detected.classifier}</classifier>
  </dependency>
</dependencies>

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

## Build

This project provides OpenDAL Java bindings with artifact name `opendal-java`. It depends on JDK 8 or later.

You can use Maven to build both Rust dynamic lib and JAR files with one command now:

```shell
mvn clean package -DskipTests=true
```

## Run tests

Currently, all tests are written in Java. It contains the Cucumber feature tests and other unit tests.

You can run tests with the following command:

```shell
mvn clean verify
```

## Todos

- [ ] ReadMe for usage
- [ ] Development/Contribution guide.
- [ ] Cross-platform build for release build.
