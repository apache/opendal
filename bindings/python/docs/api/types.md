# Types

This page documents all types in OpenDAL.

```{eval-rst}
.. automodule:: opendal.types
   :members:
   :exclude-members: EntryMode
   :show-inheritance:

.. autoclass:: opendal.types.EntryMode
   :members:

   .. py:attribute:: Dir

      The entry is a directory and can be listed.

   .. py:attribute:: File

      The entry is a file and has data to read.

   .. py:attribute:: Unknown

      The mode of the entry is unknown.

.. autodata:: opendal.options.ChecksumAlgorithm

.. autoclass:: opendal.options.OpenKwargs
```
