# Apache OpenDAL™ Python binding

The Python binding exposes Apache OpenDAL's unified storage API to Python
applications. Use the guides on the main OpenDAL website for task-oriented
documentation, and use this site for Python examples and API details.

::::{grid} 1 2 2 2
:gutter: 3

:::{grid-item-card} Basic example
:link: examples/basic
:link-type: doc

Install the package and complete a local read and write workflow.
:::

:::{grid-item-card} API reference
:link: api/operator
:link-type: doc

Look up operators, files, layers, exceptions, and supporting types.
:::

:::{grid-item-card} User guide
:link: https://opendal.apache.org/docs/bindings/python/

Learn how to connect storage services and prepare an application for production.
:::

:::{grid-item-card} Service configuration
:link: api/config
:link-type: doc

Review the typed configuration accepted by each storage service.
:::
::::

## Installation

```bash
pip install opendal
```

## Local Usage
Developer must set two required arguments to work with files locally:
- `scheme`: which should be specified as `fs`
- `root`: where OpenDAL considers the root of the directory for operations will be.

For example in the following operator:
`opendal.Operator("fs", root="/foo")`

OpenDAL considers `/foo` to be the root of all paths, and that means that we can access paths inside of `/foo` without specifying anything else.
If `/foo` happens to contain the file `baz.txt`, we can simply call `.list("/")`

We can see this in the following example:

```python
from opendal import Operator
from pathlib import Path
import os


os.makedirs("foo", exist_ok=True)

file = Path("foo", "bar.txt")
file.touch()

op = Operator("fs", root=Path("foo").name)
for file in op.list("/"):
    print(file)
```

When running we get the following output:

```
/
bar.txt
```

If we want full access to our file system, we can specify a root of `"/"`, but note that for any operations you will always need to specify the full path to a file or directory.

### Reading
There are two ways to read data using OpenDAL. One way is to use the `read` method on an operator:
```python
from opendal import Operator
from pathlib import Path
import os


os.makedirs("foo", exist_ok=True)

file = Path("foo", "bar.txt")
file.touch()
file.write_text("baz")


op = Operator("fs", root=Path("foo").name)

data = op.read(file.name)
print(data)
```

Yields the following:
```
b'baz'
```

Note that the output is bytes, but we can get it as a string by simply calling `.decode()` on the data we read. All reads with OpenDAL return bytes.

Now lets use the `open` method on an operator:
```python
from opendal import Operator
from pathlib import Path
import os


os.makedirs("foo", exist_ok=True)

file = Path("foo", "bar.txt")
file.touch()
file.write_text("baz")


op = Operator("fs", root=Path("foo").name)

with op.open(file.name, "rb") as f:
    print(f.read())
```

This again yields
```
b'baz'
```

Again, note that all reads with OpenDAL return bytes, so specifying a mode of `"r"` will raise an exception.


### Writing
Now that we know how to read data, let's replace the `Pathlib` code above with OpenDAL using the `write` method:
```python
from opendal import Operator
from pathlib import Path


op = Operator("fs", root=Path("foo").name)

op.write("baz.txt", "my amazing data".encode())

print(op.read("baz.txt").decode())
```

This yields the following:
```
my amazing data
```

And again, but using the `open` method:
```python
from opendal import Operator
from pathlib import Path


op = Operator("fs", root=Path("foo").name)

with op.open("baz.txt", "wb") as f:
    f.write("my amazing data".encode())

print(op.read("baz.txt").decode())
```

This again yields:

```
my amazing data
```

Again, note that all writing happens in bytes and a mode of `"w"` will raise an exception.

## Async
OpenDAL supports async operation on all operator methods. One can simply replace the `Operator` with an `AsyncOperator` and await on method calls. The below example illustrates this behavior:

Standard API:

```python
import opendal

op = opendal.Operator("fs", root="/tmp")
op.write("test.txt", b"Hello World")
print(op.read("test.txt"))
print(op.stat("test.txt").content_length)
```

Async API equivalent:

```python
import asyncio

async def main():
    op = opendal.AsyncOperator("fs", root="/tmp")
    await op.write("test.txt", b"Hello World")
    print(await op.read("test.txt"))

asyncio.run(main())
```

```{toctree}
:hidden:
:caption: Examples
:maxdepth: 1

Basic <examples/basic>
Pandas <examples/pandas>
Polars <examples/polars>
```

```{toctree}
:hidden:
:caption: API reference
:maxdepth: 2

api/async_file
api/async_operator
api/capability
api/config
api/exceptions
api/file
api/layers
api/operator
api/types
```
