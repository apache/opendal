## AzblobConfig

### `container`: `str`

The container name of Azblob service backend.

### `root`: `str`

The root of Azblob service backend.

All operations will happen under this root.

### `endpoint`: `str`

The endpoint of Azblob service backend.

Endpoint must be full uri, e.g.

- Azblob: `https://accountname.blob.core.windows.net`
- Azurite: `http://127.0.0.1:10000/devstoreaccount1`

### `account_name`: `str`

The account name of Azblob service backend.

### `account_key`: `str`

The account key of Azblob service backend.

### `encryption_key`: `str`

The encryption key of Azblob service backend.

### `encryption_key_sha256`: `str`

The encryption key sha256 of Azblob service backend.

### `encryption_algorithm`: `str`

The encryption algorithm of Azblob service backend.

### `sas_token`: `str`

The sas token of Azblob service backend.

### `batch_max_operations`: `_int`

The maximum batch operations of Azblob service backend.

