# Layers

This page documents all layers in OpenDAL.

```{eval-rst}
.. automodule:: opendal.layers
   :members: Layer, CapabilityOverrideLayer
   :show-inheritance:

.. autoclass:: opendal.layers.ConcurrentLimitLayer
   :show-inheritance:

   .. py:method:: __new__(limit)
      :staticmethod:

      :param limit: Maximum number of concurrent operations. Must be greater than zero.

.. autoclass:: opendal.layers.MimeGuessLayer
   :show-inheritance:

   .. py:method:: __new__()
      :staticmethod:

      Create a MIME guessing layer.

.. autoclass:: opendal.layers.RetryLayer
   :show-inheritance:

   .. py:method:: __new__(max_times=None, factor=None, jitter=False, max_delay=None, min_delay=None)
      :staticmethod:

      :param max_times: Maximum number of retry attempts. Defaults to 3.
      :param factor: Finite backoff factor of at least 1.0. Defaults to 2.0.
      :param jitter: Whether to apply jitter to the backoff.
      :param max_delay: Maximum delay in seconds. Must be finite and non-negative. Defaults to 60.0.
      :param min_delay: Minimum delay in seconds. Must be finite and non-negative. Defaults to 1.0.

.. autoclass:: opendal.layers.TimeoutLayer
   :show-inheritance:

   .. py:method:: __new__(timeout=None, io_timeout=None)
      :staticmethod:

      :param timeout: Positive finite timeout in seconds for control operations. Defaults to 60.0.
      :param io_timeout: Positive finite timeout in seconds for I/O operations. Defaults to 10.0.
```
