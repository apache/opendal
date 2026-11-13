# Baidu Netdisk

Baidu Netdisk Open Platform support, using the official xpan APIs.

Set `root` to `/apps/<application name>/` or a directory below it, and set
`access_token` to a token authorized for `basic,netdisk`.

```no_run
use opendal_core::Operator;
use opendal_service_baidu_netdisk::BaiduNetdisk;

let operator = Operator::new(BaiduNetdisk::default()
    .root("/apps/MyApp/data/")
    .access_token("access-token"))?;
# Ok::<(), opendal_core::Error>(())
```

The service supports stat, list, ranged reads, create directory, write, copy,
rename and delete. Writes require the complete file in memory before upload,
use 4 MiB blocks, and are limited to 4 GiB. Streaming writes and append are not
supported. Account membership affects download speed and server limits.

Baidu rejects some file name characters at the final file creation step.
Live tests accept ordinary Chinese and accented Latin names but reject the
emoji `🐂🍺` and supplementary character `𠮷` with error `-7`. The service preserves
the requested name and reports the rejected characters when the API supplies
them; callers must explicitly choose another name.

Rename uses server-side copy followed by delete because Baidu's move API rejects
existing destinations even when overwrite is requested. It is not atomic; if
deleting the source fails, the operation returns an error and both files remain.

Copy and rename accept files only, including when a directory path omits the
trailing slash. Copy, rename and write reject existing directory destinations.
Non-recursive delete rejects non-empty directories. Use
`delete_with("dir/").recursive(true)` to delete a directory and its contents.
Without the trailing slash, recursive deletion removes all matching paths in
the parent directory, including matching directories and their contents.
Recursive deletion checks that paths ending in `/` refer to directories.
The emptiness check and deletion are separate requests. Files added between
the check and deletion can also be removed by Baidu's recursive delete API.
Path-type checks and mutations are also separate requests; concurrent
changes to checked paths are not protected by an atomic type check.

For automatic renewal, configure `client_id` (AppKey), `client_secret` (SecretKey),
and `refresh_token` together. An initial `access_token` is used until the API
reports it expired; subsequent tokens refresh before expiration. Refresh tokens
are single-use. Use `on_token_refresh` to persist the new access and refresh
tokens before restarting; without it, rotation is retained only in memory.
A failed or cancelled refresh requires a new authorization. Independent operators
must not use the same refresh token concurrently.

Official documentation: <https://pan.baidu.com/union/doc/>.

Licensed under the Apache License, Version 2.0.
