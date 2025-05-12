## SqliteConfig

### `connection_string`: `str`

Set the connection_string of the sqlite service.

This connection string is used to connect to the sqlite service.

The format of connect string resembles the url format of the sqlite client:

- `sqlite::memory:`
- `sqlite:data.db`
- `sqlite://data.db`

For more information, please visit <https://docs.rs/sqlx/latest/sqlx/sqlite/struct.SqliteConnectOptions.html>.

### `table`: `str`

Set the table name of the sqlite service to read/write.

### `key_field`: `str`

Set the key field name of the sqlite service to read/write.

Default to `key` if not specified.

### `value_field`: `str`

Set the value field name of the sqlite service to read/write.

Default to `value` if not specified.

### `root`: `str`

set the working directory, all operations will be performed under it.

default: "/"

