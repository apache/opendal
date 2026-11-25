# Baidu Netdisk behavior tests

Create a personal software application in the [Baidu Netdisk Open Platform](https://pan.baidu.com/union/console/applist)
and follow the [authorization flow](https://pan.baidu.com/union/doc/al0rwqzzl) with the `basic,netdisk` OAuth scope.

Each contributor can run the suite with their own development account and
application. Keep the AppKey, SecretKey and tokens in a private credential
store. For each run, supply a current access token and a dedicated directory
under the application's root, for example `/apps/MyApp/opendal-tests/`.
Keep the trailing slash. The testkit appends a random subdirectory and writes
real files there. Remove that run directory after checking the results.

Set only `OPENDAL_BAIDU_NETDISK_ACCESS_TOKEN` and `OPENDAL_BAIDU_NETDISK_ROOT`
for this service in the shell or local `.env`, then run:

```sh
cd core
OPENDAL_TEST=baidu-netdisk OPENDAL_TEST_IO_TIMEOUT=300 cargo test --locked --test behavior --features tests,services-baidu-netdisk -- --test-threads=1
```

Only the access token reaches the test processes. Renew it before a run when
necessary. Have one credential owner exchange the latest refresh token and
atomically save the new token pair before starting another run. If the exchange
outcome is unknown or the latest refresh token was lost, authorize the account
again. Each contributor can obtain a separate grant without sharing credentials.

The shared emoji copy case skips Baidu. The Baidu-specific filename case
checks that those emoji names are rejected, then copies a Chinese filename
to an accented Latin filename and verifies the contents.
