# OpenDAL Dependencies

Please visit every package's `DEPENDENCIES.md` for a full list of dependencies.

## Notes

- `ring` has complex licenses that can't be detected by `cargo deny` correctly. We can use it as `ISC AND MIT AND OpenSSL`.
- Some packages like `binding/golang` replies on opendal c library only, so they don't have other extra runtime dependencies.

## Check

To check and generate dependencies list of any package of opendal, please run:

```bash
python ./scripts/dependencies.py
```
