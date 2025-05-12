## MysqlConfig

### `connection_string`: `str`

This connection string is used to connect to the mysql service. There are url based formats.

The format of connect string resembles the url format of the mysql client.
The format is: `[scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...`

- `mysql://user@localhost`
- `mysql://user:password@localhost`
- `mysql://user:password@localhost:3306`
- `mysql://user:password@localhost:3306/db`

For more information, please refer to <https://docs.rs/sqlx/latest/sqlx/mysql/struct.MySqlConnectOptions.html>.

### `table`: `str`

The table name for mysql.

### `key_field`: `str`

The key field name for mysql.

### `value_field`: `str`

The value field name for mysql.

### `root`: `str`

The root for mysql.

