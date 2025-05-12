## PostgresqlConfig

### `root`: `str`

Root of this backend.

All operations will happen under this root.

Default to `/` if not set.

### `connection_string`: `str`

The URL should be with a scheme of either `postgres://` or `postgresql://`.

- `postgresql://user@localhost`
- `postgresql://user:password@%2Fvar%2Flib%2Fpostgresql/mydb?connect_timeout=10`
- `postgresql://user@host1:1234,host2,host3:5678?target_session_attrs=read-write`
- `postgresql:///mydb?user=user&host=/var/lib/postgresql`

For more information, please visit <https://docs.rs/sqlx/latest/sqlx/postgres/struct.PgConnectOptions.html>.

### `table`: `str`

the table of postgresql

### `key_field`: `str`

the key field of postgresql

### `value_field`: `str`

the value field of postgresql

