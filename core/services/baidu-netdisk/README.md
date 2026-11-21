# Baidu Netdisk

Baidu Netdisk Open Platform support, using the official xpan APIs.

Set `root` to `/apps/<application name>/` or a directory below it, and set
`access_token` to a token authorized for `basic,netdisk`.

```rust,ignore
use opendal_core::Operator;
use opendal_service_baidu_netdisk::BaiduNetdisk;

let operator = Operator::new(BaiduNetdisk::default()
    .root("/apps/MyApp/data/")
    .access_token("access-token"))?;
```

The service supports stat, list, ranged reads, create directory, write, copy
and delete. Baidu requires all block hashes before it opens an upload. Each
writer accepts one payload of up to 16 MiB, retains it until close or abort,
and uploads it in 4 MiB blocks. Larger writes and a second write to the same
writer return `Unsupported`. Account membership affects download speed.

Baidu accepts ordinary Chinese and accented Latin names. The metadata lookup
before upload rejects the tested emoji and supplementary character `𠮷` with
parameter error `31023`. The create API can also reject names with error `-7`
and identify the characters in `illegal_chars`. The service preserves the path
and returns the API error.

Copy accepts file sources and rejects existing directory destinations. Baidu's
native move rejects an existing destination, so the service omits rename.

Baidu's delete API removes directory trees. Ordinary deletion of an existing
directory returns `Unsupported`, including when it is empty. Use
`delete_with("dir/").recursive(true)` to remove a directory and its contents.
Without the trailing slash, recursive deletion removes all matching paths in
the parent directory, including matching directory trees. File deletion is
path-based. Applications must serialize changes that replace a file with a
directory at the same path during deletion.

For automatic renewal in Rust, configure `client_id` (AppKey), `client_secret`
(SecretKey), `refresh_token`, and `on_token_refresh` together. The callback
must atomically persist the new access and refresh tokens before returning
success. Clones of one operator serialize refresh and persistence. A failed
callback blocks use of the new pair and subsequent requests retry persistence.
Restart with the most recently persisted pair. Use one owner per grant across
operators and processes. If a crash or interrupted request loses the response
after Baidu consumes the refresh token, obtain a new authorization.

For configuration via bindings or environment variables, supply an access token
and manage renewal outside OpenDAL.

Official documentation: <https://pan.baidu.com/union/doc/>.

Licensed under the Apache License, Version 2.0.
