# Operator

```{eval-rst}
.. autoclass:: opendal.Operator
   :members:
   :exclude-members: open

   .. py:method:: __new__(scheme, **kwargs)
      :staticmethod:

      Create a blocking operator.

      :param scheme: The service scheme as a string or a ``Scheme`` value.
      :param kwargs: Service options. Path-like values are converted to strings.

   .. automethod:: open(path: str | os.PathLike[str], mode: str, **kwargs: typing.Unpack[~opendal.options.OpenKwargs])
```

## Get a stored checksum

With a version-aware service, pass the object version ID `v` to request the
full-object checksum for that version.

```python
stored = op.stat(key, version=v, checksum=True).checksum
if stored is not None:
    algorithm, digest = stored
```

The digest contains raw bytes. The property is `None` if the service does not
provide a full-object checksum. `stat` does not read the object data to obtain it.
