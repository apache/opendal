## IcloudConfig

### `root`: `str`

root of this backend.

All operations will happen under this root.

default to `/` if not set.

### `apple_id`: `str`

apple_id of this backend.

apple_id must be full, mostly like `example@gmail.com`.

### `password`: `str`

password of this backend.

password must be full.

### `trust_token`: `str`

Session

token must be valid.

### `ds_web_auth_token`: `str`

ds_web_auth_token must be set in Session

### `is_china_mainland`: `_bool`

enable the china origin
China region `origin` Header needs to be set to "https://www.icloud.com.cn".

otherwise Apple server will return 302.

