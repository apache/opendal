# OpenDAL Java Bindings

[![Maven Central](https://img.shields.io/maven-central/v/org.apache.opendal/opendal-java.svg?logo=Apache+Maven&logoColor=blue)](https://central.sonatype.com/search?q=opendal-java&smo=true)
[![Website](https://img.shields.io/badge/opendal-OpenDAL_Website-red?logo=Apache&logoColor=red)](https://opendal.apache.org/docs/java/)

![](https://github.com/apache/incubator-opendal/assets/5351546/87bbf6e5-f19e-449a-b368-3e283016c887)

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
</dependency>
<dependency>
  <groupId>org.apache.opendal</groupId>
  <artifactId>opendal-java</artifactId>
  <version>${opendal.version}</version>
  <classifier>${os.detected.classifier}</classifier>
</dependency>
```

Note that the dependency without classifier ships all classes and resources except the "opendal_java" shared library. And those with classifier bundle only the shared library.

For downstream usage, it's recommended:

* Depend on the one without classifier to write code; 
* Depend on the classified ones with "test" for testing.

To load the shared library correctly, you can choose one of the following approaches:

* Append the classified JARs to the classpath at the runtime;
* Depend on the classified JARs and build a fat JAR (You may need to depend on all the provided classified JARs for running on multiple platforms);
* Build your own "opendal_java" shared library and specify "-Djava.library.path" to the folder containing that shared library.

## Build

This project provides OpenDAL Java bindings with artifact name `opendal-java`. It depends on JDK 8 or later.

You can use Maven to build both Rust dynamic lib and JAR files with one command now:

```shell
./mvnw clean package -DskipTests=true
```

## Setup Tests

Please copy `{project.rootdir}/.env.example` to `{project.rootdir}/.env` and change the values on need.

Take `fs` for example, we need to enable bench on `fs` on `/tmp`.

```dotenv
OPENDAL_FS_TEST=false
OPENDAL_FS_ROOT=/path/to/dir
```

into

```dotenv
OPENDAL_FS_TEST=on
OPENDAL_FS_ROOT=/tmp
```

Notice: The default will skip all benches if the env is not set.

## Run tests

Currently, all tests are written in Java.

You must set the `OPENDAL_TEST_SCHEMA` parameter to specify the backends to be tested.

You can run all available backends tests with the following command:

```shell
./mvnw clean verify -DOPENDAL_TEST_SCHEMA=redis -Dcargo-build.features=services-redis
```

Test specific backend(such as `fs`).

```shell
./mvnw test -Dtest=org.apache.opendal.OperatorTest -DOPENDAL_TEST_SCHEMA=fs
```

Test specific backend(such as `redis`).

```shell
./mvnw test -Dtest=org.apache.opendal.OperatorTest -DOPENDAL_TEST_SCHEMA=redis -Dcargo-build.features=services-redis
```

> **Note:**
> 
> The `-Dcargo-build.features=services-redis` argument is a temporary workaround. See also:
> 
> * https://github.com/apache/incubator-opendal/pull/3060
> * https://github.com/apache/incubator-opendal/issues/3066

Additionally, this project uses [spotless](https://github.com/diffplug/spotless) for code formatting so that all developers share a consistent code style without bikeshedding on it.

You can apply the code style with the following command::

```shell
./mvnw spotless:apply
```
