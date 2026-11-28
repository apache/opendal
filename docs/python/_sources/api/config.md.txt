# Config

Use a typed configuration to construct an `Operator` or `AsyncOperator`. The
service config's `scheme` selects the storage service. Use `context` for
per-operator HTTP settings and `layers` to apply declarative layers in order.

```python
from datetime import timedelta

from opendal import Operator
from opendal.config import ContextConfig, HttpContextConfig, RetryLayerConfig, S3Config

op = Operator.from_config(
    S3Config(scheme="s3", bucket="my-bucket"),
    context=ContextConfig(http=HttpContextConfig(connect_timeout=timedelta(seconds=5))),
    layers=[RetryLayerConfig(name="retry", max_times=3)],
)
```

<!-- Preserve the module fragment used by the published Zensical reference. -->
<div id="opendal.config"></div>

```{eval-rst}
.. automodule:: opendal.config
   :members:
   :show-inheritance:
```
