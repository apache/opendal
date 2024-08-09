# Behavior tests for OpenDAL™ Cloud Filter Integration

Behavior tests are used to make sure every service works correctly.

`cloudfilter_opendal` is readonly currently, so we assume `fixtures/data` is the root of the test data.

## Run

```pwsh
cd .\integrations\cloudfilter
$env:OPENDAL_TEST='fs'; $env:OPENDAL_FS_ROOT='../../fixtures/data'; $env:OPENDAL_DISABLE_RANDOM_ROOT='true'; cargo test --test behavior
```
