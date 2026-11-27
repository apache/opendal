# AsyncOperator

```{eval-rst}
.. autoclass:: opendal.AsyncOperator
   :members:
   :exclude-members: open

   .. py:method:: __new__(scheme, **kwargs)
      :staticmethod:

      Create an async operator.

      :param scheme: The service scheme as a string or a ``Scheme`` value.
      :param kwargs: Service options. Path-like values are converted to strings.

   .. automethod:: open(path: str | os.PathLike[str], mode: str, **kwargs: typing.Unpack[~opendal.options.OpenKwargs])
```
