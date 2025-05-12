## OssConfig

### `bucket`: `str`

Bucket for oss.

### `root`: `str`

Root for oss.

### `endpoint`: `str`

Endpoint for oss.

### `presign_endpoint`: `str`

Presign endpoint for oss.

### `enable_versioning`: `_bool`

is bucket versioning enabled for this bucket

### `server_side_encryption`: `str`

Server side encryption for oss.

### `server_side_encryption_key_id`: `str`

Server side encryption key id for oss.

### `allow_anonymous`: `_bool`

Allow anonymous for oss.

### `access_key_id`: `str`

Access key id for oss.

### `access_key_secret`: `str`

Access key secret for oss.

### `batch_max_operations`: `_int`

**deprecated**: Please use `delete_max_size` instead of `batch_max_operations`The size of max batch operations.

### `delete_max_size`: `_int`

The size of max delete operations.

### `role_arn`: `str`

If `role_arn` is set, we will use already known config as source
credential to assume role with `role_arn`.

### `role_session_name`: `str`

role_session_name for this backend.

### `oidc_provider_arn`: `str`

`oidc_provider_arn` will be loaded from

- this field if it's `is_some`
- env value: [`ALIBABA_CLOUD_OIDC_PROVIDER_ARN`]

### `oidc_token_file`: `str`

`oidc_token_file` will be loaded from

- this field if it's `is_some`
- env value: [`ALIBABA_CLOUD_OIDC_TOKEN_FILE`]

### `sts_endpoint`: `str`

`sts_endpoint` will be loaded from

- this field if it's `is_some`
- env value: [`ALIBABA_CLOUD_STS_ENDPOINT`]

