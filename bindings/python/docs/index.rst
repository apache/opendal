.. OpenDAL documentation master file, created by
   sphinx-quickstart on Fri Apr 11 10:19:08 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Apache OpenDAL™ Python binding
==============================


Installation
------------

.. code-block:: shell

    pip install opendal

Usage
-----

.. code-block:: python

    import opendal

    op = opendal.Operator("fs", root="/tmp")
    op.write("test.txt", b"Hello World")
    print(op.read("test.txt"))
    print(op.stat("test.txt").content_length)


Or using the async API:

.. code-block:: python

    import asyncio

    async def main():
        op = opendal.AsyncOperator("fs", root="/tmp")
        await op.write("test.txt", b"Hello World")
        print(await op.read("test.txt"))

    asyncio.run(main())


.. toctree::
   :maxdepth: 2
   :caption: Contents:

API reference
-------------

.. automodule:: opendal
    :members:
