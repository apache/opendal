# OpenDAL C Binding (WIP)

## Development

To run unit tests, you need [cmocka](https://cmocka.org/). 
To install `cmocka`, follow the instructions in [cmocka - INSTALL.md](https://gitlab.com/cmocka/cmocka/-/blob/master/INSTALL.md).

All in one (without clean):

```bash
make
```

Build bindings:

```bash
make build
```

Run some tests:

```bash
make build/
make test
```

Clean targets:

```bash
make clean
```

## License

[Apache v2.0](https://www.apache.org/licenses/LICENSE-2.0)
