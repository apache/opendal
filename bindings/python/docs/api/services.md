# Services

Each service exposes a typed configuration class. Pass an instance to
`Operator.from_config` or `AsyncOperator.from_config`:

```python
from opendal import Operator
from opendal.services import S3Config

op = Operator.from_config(S3Config(bucket="my-bucket"))
```

::: opendal.services
    options:
      show_source: false
      heading_level: 2
      show_signature: true
      show_signature_annotations: true
      separate_signature: true
      merge_init_into_class: true
