## GcsConfig

### `bucket`: `str`

bucket name

### `root`: `str`

root URI, all operations happens under `root`

### `endpoint`: `str`

endpoint URI of GCS service,
default is `https://storage.googleapis.com`

### `scope`: `str`

Scope for gcs.

### `service_account`: `str`

Service Account for gcs.

### `credential`: `str`

Credentials string for GCS service OAuth2 authentication.

### `credential_path`: `str`

Local path to credentials file for GCS service OAuth2 authentication.

### `predefined_acl`: `str`

The predefined acl for GCS.

### `default_storage_class`: `str`

The default storage class used by gcs.

### `allow_anonymous`: `_bool`

Allow opendal to send requests without signing when credentials are not
loaded.

### `disable_vm_metadata`: `_bool`

Disable attempting to load credentials from the GCE metadata server when
running within Google Cloud.

### `disable_config_load`: `_bool`

Disable loading configuration from the environment.

### `token`: `str`

A Google Cloud OAuth2 token.

Takes precedence over `credential` and `credential_path`.

