- Proposal Name: `config`
- Start Date: 2023-09-27
- RFC PR: [apache/incubator-opendal#3197](https://github.com/apache/incubator-opendal/pull/3197)
- Tracking Issue: [apache/incubator-opendal#0000](https://github.com/apache/incubator-opendal/issues/0000)

# Summary

Expose services config to the user. 

# Motivation

OpenDAL have two ways to configure services: builder and map.

`Builder` allows user to configure services in builder pattern:

```rust
// Create fs backend builder.
let mut builder = Fs::default();
// Set the root for fs, all operations will happen under this root.
builder.root("/tmp");

// Build an `Operator` to start operating the storage.
let op: Operator = Operator::new(builder)?.finish();
```

The benefit of builder is that it is type safe and easy to use. However, it is not flexible enough to configure services. Users will need to create a new builder for each service they want to configure and convert them from users input to builder's API one by one.

Take one of our users real code as an example:

```rust
let mut builder = services::S3::default();

// Credential.
builder.access_key_id(&cfg.access_key_id);
builder.secret_access_key(&cfg.secret_access_key);
builder.security_token(&cfg.security_token);
builder.role_arn(&cfg.role_arn);
builder.external_id(&cfg.external_id);

// Root.
builder.root(&cfg.root);

// Disable credential loader
if cfg.disable_credential_loader {
    builder.disable_config_load();
    builder.disable_ec2_metadata();
}

// Enable virtual host style
if cfg.enable_virtual_host_style {
    builder.enable_virtual_host_style();
}
```

`Map` allows user to configure services in a string based `HashMap`:

```rust
let map = HashMap::from([
  // Set the root for fs, all operations will happen under this root.
  ("root".to_string(), "/tmp".to_string()),
]);

// Build an `Operator` to start operating the storage.
let op: Operator = Operator::via_map(Scheme::Fs, map)?;
```

It's much simple since users can configure all services in one map. However, it is not type safe and not easy to use. Users will need to convert their input to string and make sure the key is correct. And breaking changes could happen silently.

This is one of our limitations: We need a way to configure services that is type safe, easy to use and flexible. The other one is that there is no way for users to fetch the config of a service after it's built. This behavior makes it much complex for users to implement logic that changing the root path of a service dynamically.

Our users have to wrap all our configs into an enum and store it in their own struct:

```rust
pub enum StorageParams {
    Azblob(StorageAzblobConfig),
    Fs(StorageFsConfig),
    Ftp(StorageFtpConfig),
    Gcs(StorageGcsConfig),
    Hdfs(StorageHdfsConfig),
    Http(StorageHttpConfig),
    Ipfs(StorageIpfsConfig),
    Memory,
    Moka(StorageMokaConfig),
    Obs(StorageObsConfig),
    Oss(StorageOssConfig),
    S3(StorageS3Config),
    Redis(StorageRedisConfig),
    Webhdfs(StorageWebhdfsConfig),
    Cos(StorageCosConfig),
}
```

So I propose to expose services config to the users, allowing them to work on config structs directly and fetch the config at runtime.

# Guide-level explanation

First of all, we will add config struct for each service. For example, `Fs` will have a `FsConfig` struct and `S3` will have a `S3Config`. The config's filed is public and non-exhaustive.

```rust
#[non_exhaustive]
pub struct S3Config {
  pub root: Option<String>,
  pub bucket: String,
  pub endpoint: Option<String>,
  pub region: Option<String>,
  ...
}
```

Then, we will add a `Config` enum that contains all the config structs. The enum is public and non-exhaustive too.

```rust
#[non_exhaustive]
pub enum Config {
  Fs(FsConfig)
  S3(S3Config),
  Custom(&'static str, HashMap<String, String>),
}
```

Especially, we will add a `Custom` variant to the enum. This variant aligns with `Scheme::Custom(name)` and allows users to configure custom services.

At `Operator` level, we will add `from_config` and `via_config` methods.

```rust
impl Operator {
  pub fn via_config(cfg: impl Into<Config>) -> Result<Operator> {}
}
```

And `OperatorInfo` will provide a new API called `config()`:

```rust
impl OperatorInfo {
  pub fn config(&self) -> Config {}
}
```

Users can use `config()` to fetch the config of a service at runtime and construct a new operator based on needs.

# Reference-level explanation

Every services will have a `XxxConfig` struct.

`XxxConfig` will implement the following things:

- `Default` trait: All config fields will have a default value.
- `Serialize` trait.
- `Deserialize` trait.
- `FromMap` trait: Convert from a `HashMap<String, String>`.
- `Into<Config>` trait: All service config can be converted to `Config` enum.

All config fields will be public and non-exhaustive, allowing users to build config this way:

```rust
let s3 = S3Config {
  bucket: "test".to_string(),
  endpoint: Some("http://localhost:9000".to_string()),
  ..Default::default()
}
```

Existing builder's public API will not be changed, but it's internal implementation will be changed to use `XxxConfig` instead. Type that can't be represents as `String` like `Box<dyn AwsCredentialLoad>` and `HttpClient` will be kept in `Builder` as before.

# Drawbacks

This change makes OpenDAL's public API surface much larger.

# Rationale and alternatives

## Move `root` out of service config to operator level

There is another way to solve the problem: [Move `root` out of service config to operator level](https://github.com/apache/incubator-opendal/issues/3151).

We can move `root` out of the service config and put it in `Operator` level. This way, users can configure `root` for all services in one place. However, this is a large breaking changes and users will need to maintain the `root` logic everywhere.

# Prior art

None.

# Unresolved questions

None.

# Future possibilities

## Implement `FromStr` for `Config`

We can implement `FromStr` for `Config` so that users can parse a config from a string.

```rust
let cfg = Config::from_str("s3://bucket/path/to/file?access_key_id=xxx&secret_access_key=xxx")?;
```

## Implement `Serialize` and `Deserialize` for `Config`

We can implement `Serialize` and `Deserialize` for `Config` so that users can serialize and deserialize a config.

```rust
// Serialize
let bs = serde_json::to_vec(&cfg)?;
// Deserialize
let cfg: Config = serde_json::from_slice(&bs)?;
```
