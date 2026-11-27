# Contributing to Apache OpenDAL™ Java Bindings

This document covers building, testing, and styling the Java binding. For usage,
see the [user guide](https://opendal.apache.org/docs/bindings/java).

## Build

This project provides OpenDAL Java bindings with artifact name `opendal`. It depends on JDK 8 or later.

You can use Maven to build both the Rust dynamic lib and JAR files with one command:

```shell
./mvnw clean package -DskipTests=true
```

## Run tests

Currently, all tests are written in Java.

You can run the base tests with the following command:

```shell
./mvnw clean verify
```

Run the JNI panic regression tests with the test-only native fixture enabled.

```shell
./mvnw test -Dtest=NativePanicTest -Dopendal.test-native-panics=true
```

## Code style

This project uses [spotless](https://github.com/diffplug/spotless) for code formatting so that all developers share a consistent code style without bikeshedding on it.

You can apply the code style with the following command:

```shell
./mvnw spotless:apply
```

## Code generation

`ServiceConfig.java`, `Capability.java`, `Metadata.java`, `*Options.java`, `src/capability.rs`, `src/metadata.rs`, and `src/options.rs` are generated from OpenDAL core definitions. After changing service configurations, `core/core/src/types/capability.rs`, `core/core/src/types/metadata.rs`, or `core/core/src/types/options.rs`, regenerate them from the repository root:

```shell
just generate java
```

## Run behavior tests

Services behavior tests read necessary configs from env vars or the `.env` file.

You can copy [.env.example](/.env.example) to `${project.rootdir}/.env` and change the values on need, or directly set env vars with `export KEY=VALUE`.

Take `fs` for example, we need to enable bench on `fs` on `/tmp/`:

```properties
OPENDAL_TEST=fs
OPENDAL_FS_ROOT=/tmp/
```

You can run service behavior tests of enabled services with the following command:

```shell
./mvnw test -Dtest="behavior.*Test"
```

## JAR classifiers

- The default JAR contains the Java classes and resources.
- Each platform classifier bundles one `opendal_java` JNI library and licensing files.
- The `all` classifier contains the Java classes, licensing files, and JNI libraries
  for all seven published platforms. Maven consumers can declare one `opendal`
  dependency with `<classifier>all</classifier>` to compile and run. The download
  is about 200 MB. The default JAR plus a platform classifier is smaller.

For downstream usage, it's recommended:

* Depend on the one without classifier to write code;
* Depend on the classified ones with "test" for testing.

To load the shared library correctly, you can choose one of the following approaches:

* Append the classified JARs to the classpath at the runtime;
* Depend on the classified JARs and build a fat JAR (You may need to depend on all the provided classified JARs for running on multiple platforms);
* Build your own "opendal_java" shared library and specify "-Djava.library.path" to the folder containing that shared library.

The musl native library is dynamically linked against musl libc and the GCC runtime. On Alpine Linux, install `libgcc` before loading the native library.

### Assemble the `all` JAR

With Python 3.11 or later, run the `all-natives` profile from `bindings/java`.
Point `all.natives.dir` at the directory containing the seven platform JARs
built from the same sources and version as the Java classes.

```shell
./mvnw -Pall-natives -Dall.natives.dir=/path/to/platform-jars -DskipTests package
```

The inputs must follow the `opendal-${version}-${classifier}.jar` naming
convention for `linux-x86_64`, `linux-x86_64-musl`, `linux-aarch_64`,
`linux-aarch_64-musl`, `osx-x86_64`, `osx-aarch_64`, and `windows-x86_64`.
The profile builds the default and `all` JARs in `target/` using the supplied JNI
libraries.
