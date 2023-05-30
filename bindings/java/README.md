# OpenDAL Java Bindings

[![Maven Central](https://img.shields.io/maven-central/v/org.apache.opendal/opendal-java.svg?logo=Apache+Maven&logoColor=blue)](https://central.sonatype.com/search?q=opendal-java&smo=true)
[![Website](https://img.shields.io/badge/opendal-OpenDAL_Website-red?logo=Apache&logoColor=red)](https://opendal.apache.org/docs/java/)


## Getting Started

This project is built upon the native OpenDAL lib. And it is released for multiple platforms that you can use a classifier to specify the platform you are building the application on.

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

Then add the dependency to `opendal-java` as following:

```xml
<dependency>
  <groupId>org.apache.opendal</groupId>
  <artifactId>opendal-java</artifactId>
  <version>${opendal.version}</version>
  <classifier>${os.detected.classifier}</classifier>
</dependency>
```

If you'd rather like the latest snapshots of the upcoming major version, use ASF Maven snapshot repository:

```xml
<repositories>
    <repository>
        <id>apache.snapshots</id>
        <name>Apache Snapshot Repository</name>
        <url>https://repository.apache.org/snapshots</url>
    </repository>
</repositories>
```

... and declare the appropriate dependency version:

```xml
<dependency>
  <groupId>org.apache.opendal</groupId>
  <artifactId>opendal-java</artifactId>
  <version>${opendal.snapshot-version}</version>
  <classifier>${os.detected.classifier}</classifier>
</dependency>
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

Additionally, this project uses [spotless](https://github.com/diffplug/spotless) for code formatting so that all developers share a consistent code style without bikeshedding on it.

You can apply the code style with the following command::

```shell
mvn spotless:apply
```
