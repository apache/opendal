---
title: Verify a release candidate
sidebar_position: 4
---

To verify a release candidate, the following checklist could be used:

- [ ] Download links are valid.
- [ ] Checksums and signatures.
- [ ] LICENSE/NOTICE files exist.
- [ ] No unexpected binary files.
- [ ] All source files have ASF headers.
- [ ] Can compile from source.

:::note

It is NOT necessary to run all checks to cast a vote for a release candidate.

However, you should clearly state which checks you did. The release manager needs to ensure that each check was done.

:::

## Download links are valid

To verify the release candidate, you need to download the release candidate from the [dist](https://dist.apache.org/repos/dist/dev/opendal/) directory.

Use the following command to download all artifacts, replace "${release_version}-${rc_version}" with the version ID of the version to be released:

```shell
svn co https://dist.apache.org/repos/dist/dev/opendal/${release_version}-${rc_version}/
```

## Checksums and signatures

The release candidate should have a checksum and signature file.

For example, if the release candidate is `0.45.0-rc1`, the checksum and signature file should be:

```
https://dist.apache.org/repos/dist/dev/opendal/0.45.0-rc1/apache-opendal-0.45.0-rc1-src.tar.gz.sha512
https://dist.apache.org/repos/dist/dev/opendal/0.45.0-rc1/apache-opendal-0.45.0-rc1-src.tar.gz.asc
```

### Verify checksums and signatures

GnuPG is recommended here. It can be installed with the following command:

```shell
apt-get install gnupg
# or
yum install gnupg
# or
brew install gnupg
```

Firstly, import the OpenDAL release manager's public key:

```shell
curl https://downloads.apache.org/opendal/KEYS > KEYS # Download KEYS
gpg --import KEYS # Import KEYS to local
```

Then, trust the public key:

```shell
gpg --edit-key <KEY-used-in-this-version> # Edit the key
```

It will enter the interactive mode, use the following command to trust the key:

```shell
gpg> trust
```

And then, select the level of trust, for example:

```
Please decide how far you trust this user to correctly verify other users' keys
(by looking at passports, checking fingerprints from different sources, etc.)

  1 = I don't know or won't say
  2 = I do NOT trust
  3 = I trust marginally
  4 = I trust fully
  5 = I trust ultimately
  m = back to the main menu
```

Select `5` to trust the key ultimately.

Now, we could start the verification.

We've provided a script to verify the checksum and signature of the release candidate.

The script is in the `scripts` directory of our repository.
You can download it directly from [here](https://raw.githubusercontent.com/apache/opendal/main/scripts/check.sh)
or check it out from the repository:

```shell
git clone git@github.com:apache/opendal.git
```

Run the script on a specific release candidate:

```shell
./scripts/check.sh apache-opendal-${release_version}-${rc_version}-src.tar.gz
```

You will see the following output if the verification is successful:

```
gpg: Signature made Wed 21 Jul 2021 10:00:00 AM CST
gpg:                using RSA key 0x1234567890ABCDEF
gpg: Good signature from "Xuanwo<xuanwo@apache.org" [ultimate]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1234 5678 90AB CDEF 1234  5678 90AB CDEF 1234 5678
Success to verify the gpg sign
apache-opendal-0.36.0-rc1-src.tar.gz: OK
Success to verify the checksum
```

## Check the file content of the source package

Unzip `apache-opendal-${release_version}-${rc_version}-src.tar.gz` and check the follows:

- LICENSE and NOTICE files are correct for the repository.
- All files have ASF license headers if necessary.
- Building is OK.

## Check the Maven artifacts of opendal-java

Download the artifacts from `https://repository.apache.org/content/repositories/orgapacheopendal-${maven_artifact_number}/`.

You can check the follows:

- Checksum of JARs match the bundled checksum file.
- Signature of JARs match the bundled signature file.
- JARs is reproducible locally. This means you can build the JARs on your machine and verify the checksum is the same with the bundled one.

The reproduciblility requires the same JDK distribution and the same Maven distribution. You should use [Eclipse Temurin JDK 8](https://adoptium.net/temurin/releases/?version=8) and the bundled Maven Wrapper to make the same artifacts.
