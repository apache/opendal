## AliyunDriveConfig

### `drive_type`: `str`

The drive_type of this backend.

All operations will happen under this type of drive.

Available values are `default`, `backup` and `resource`.

Fallback to default if not set or no other drives can be found.

### `root`: `str`

The Root of this backend.

All operations will happen under this root.

Default to `/` if not set.

### `access_token`: `str`

The access_token of this backend.

Solution for client-only purpose. #4733

Required if no client_id, client_secret and refresh_token are provided.

### `client_id`: `str`

The client_id of this backend.

Required if no access_token is provided.

### `client_secret`: `str`

The client_secret of this backend.

Required if no access_token is provided.

### `refresh_token`: `str`

The refresh_token of this backend.

Required if no access_token is provided.

