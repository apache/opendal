# Apache OpenDAL™ Rust Core Examples

Thank you for using OpenDAL!

Those examples are designed to help you to understand how to use OpenDAL Rust Core.

## Setup

All examples following the same setup steps:

To run this example, please copy the `.env.example`, which is at project root, to `.env` and change the values on need.

Take `fs` for example, we need to change to enable behavior test on `fs` on `/tmp`.

```dotenv
OPENDAL_FS_ROOT=/path/to/dir
```

into

```dotenv
OPENDAL_FS_ROOT=/tmp/
```
