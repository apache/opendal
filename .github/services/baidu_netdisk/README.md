# Baidu Netdisk behavior tests

Create a personal software application in the [Baidu Netdisk Open Platform](https://pan.baidu.com/union/console/applist)
and authorize it with the `basic,netdisk` OAuth scope.

Set `OPENDAL_BAIDU_NETDISK_ROOT` to a dedicated test directory beneath
`/apps/<application name>/`, and set `OPENDAL_BAIDU_NETDISK_ACCESS_TOKEN` to the
current access token. Run tests serially to avoid API rate limits:

```sh
cd core
OPENDAL_TEST=baidu-netdisk cargo test --test behavior --features tests,services-baidu-netdisk -- --test-threads=1
```

The testkit creates a random root under the configured directory. Tests modify
files there; use an application and account intended for development.
Baidu rejects the emoji filenames used by
`test_copy_file_with_non_ascii_name`, with API error `-7` at the final `create`
step. The response identifies `🐂🍺` in `illegal_chars`; the copy is never reached.
Independent live API probes accept `中文.txt` and `Français.txt`, but also reject
`𠮷.txt`. The test keeps its original emoji filenames and explicitly returns
early for Baidu with a warning, following other services' compatibility skips.
The harness reports this early return as passed; report it as skipped when
summarizing live coverage, not as verified emoji support. The service reports
rejected names without rewriting them.
