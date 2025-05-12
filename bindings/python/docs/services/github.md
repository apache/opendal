## GithubConfig

### `owner`: `str`

GitHub repo owner.

required.

### `repo`: `str`

GitHub repo name.

required.

### `root`: `str`

root of this backend.

All operations will happen under this root.

### `token`: `str`

GitHub access_token.

optional.
If not provided, the backend will only support read operations for public repositories.
And rate limit will be limited to 60 requests per hour.

