# Metrics

OpenDAL has native support for metrics.

## Metrics Layer

[MetricsLayer](https://opendal.databend.rs/opendal/layers/struct.MetricsLayer.html) will add metrics for every operation.

Enable metrics layer requires enable feature `layer-metrics`:

```rust
use anyhow::Result;
use opendal::layers::MetricsLayer;
use opendal::Operator;
use opendal::Scheme;

let _ = Operator::from_env(Scheme::Fs)
    .expect("must init")
    .layer(MetricsLayer);
```

## Metrics Output

OpenDAL is using [`metrics`](https://docs.rs/metrics/latest/metrics/) for metrics internally.

To enable metrics output, please enable one of the exporters that `metrics` supports.

Take [`metrics_exporter_prometheus`](https://docs.rs/metrics-exporter-prometheus/latest/metrics_exporter_prometheus/) as an example:

```rust
let builder = PrometheusBuilder::new();
builder.install().expect("failed to install recorder/exporter");
let handle = builder.install_recorder().expect("failed to install recorder");
let (recorder, exporter) = builder.build().expect("failed to build recorder/exporter");
let recorder = builder.build_recorder().expect("failed to build recorder");
```
