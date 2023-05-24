# OpenDAL Java Bindings

## Build

This project provide OpenDAL Java bindings with artifact name `opendal-java`. It depends on JDK 8 or later.

Thanks to [rust-maven-plugin](https://github.com/questdb/rust-maven-plugin) with can use Maven to build both Rust dynamic lib and JAR files with one command:

```shell
mvn clean package
```

## Run tests

Currently, all tests are written in Java. It contains the Cucumber feature tests and other unit tests.

You can run tests with the following command:

```shell
mvn clean verify
```

## Deploy snapshots

To deploy snapshots, you need to configure your GPG signing key and loacl Maven settings.

### GPG

1. Download GPG from http://www.gnupg.org/download/ or any other method.
2. Generate [PGP code signing keys](https://infra.apache.org/release-signing.html#generate).

> **Note**: When doing a release you will be asked for your PGP password multiple times unless you set up the gpg-agent. Mac users have reported trouble getting gpg-agent to work. You can also set your gpg-password in the Maven settings file (see below).

### Maven Settings

Your Maven settings (`$HOME/.m2/settings.xml`) file should have the following:

```xml
<settings>
    ...
    <servers>
        ...
        <server>
            <id>apache.website.svnpub</id>
            <username>Your Apache Username</username>
            <privateKey>${user.home}/.ssh/id_rsa</privateKey>
        </server>

        <server>
            <id>apache.releases.https</id>
            <username>Your Apache Username</username>
            <password>APACHE-PASSWORD-ENCODED</password>
        </server>

        <server>
            <id>apache.snapshots.https</id>
            <username>Your Apache Username</username>
            <password>APACHE-PASSWORD-ENCODED</password>
        </server>
        ...
    </servers>

    <profiles>
        <profile>
            <properties>
                ...
    	        <gpg.keyname>id-of-your-pgp-key</gpg.keyname>
                <gpg.passphrase>your-pgp-password</gpg.passphrase> <!-- use this if gpg-agent doesn't work for you -->
                ...
            </properties>
            ...
        </profile>
        ...
    </profiles>
    ...
</settings>
```

> **Note**: You can store encrypted passwords your settings.xml if you want. Read the details here: http://maven.apache.org/guides/mini/guide-encryption.html

### Deploy

Thanks to the [ASF Parent POM](https://maven.apache.org/pom/asf/), deploy a snapshot version requires only an oneliner:

```shell
mvn -P apache-release deploy
```

> **Note**: Please ensure that the project version of `opendal-java` in the `pom.xml` file ends with `-SNAPSHOT`.

## Todos

- [ ] ReadMe for usage
- [ ] Development/Contribution guide.
- [ ] Cross-platform build for release build.
