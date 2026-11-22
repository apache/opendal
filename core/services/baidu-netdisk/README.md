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
writer return `Unsupported`. A successful write returns the stored size. Use
stat to obtain the server modification time. Account membership affects download speed.

Baidu accepts ordinary Chinese and accented Latin names. The metadata lookup
before upload rejects the tested emoji and supplementary character `𠮷` with
parameter error `31023`. The create API can also reject names with error `-7`
and identify the characters in `illegal_chars`. The service preserves the path
and returns the API error.

Copy accepts file sources and rejects existing directory destinations. Baidu's
native move rejects an existing destination, so the service omits rename.

Delete targets the exact file or directory. Ordinary deletion checks a directory
for children and rejects a non-empty directory. Baidu only provides recursive
directory deletion, so the emptiness check and deletion are separate requests.
Callers must serialize writes into the directory and replacements of the target
when using ordinary deletion. An explicit recursive delete removes the target
directory and its contents. Similarly named siblings are preserved.

The caller obtains and renews the access token and builds a new operator when
the token changes. Baidu returns `PermissionDenied` for an expired token.

Official documentation: <https://pan.baidu.com/union/doc/>.

Licensed under the Apache License, Version 2.0.
