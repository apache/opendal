use ::opendal as od;
use std::time::Duration;

#[derive(ocaml::FromValue, ocaml::ToValue)]
#[ocaml::sig]
pub struct RetryLayer {
    max_times: Option<usize>,
    factor: Option<f32>,
    jitter: bool,
    max_delay: Option<f64>,
    min_delay: Option<f64>,
}

impl RetryLayer {
    pub fn build(self) -> od::layers::RetryLayer {
        let mut retry = od::layers::RetryLayer::default();

        if let Some(max_times) = self.max_times {
            retry = retry.with_max_times(max_times);
        }
        if let Some(factor) = self.factor {
            retry = retry.with_factor(factor);
        }
        if self.jitter {
            retry = retry.with_jitter();
        }
        if let Some(max_delay) = self.max_delay {
            retry = retry.with_max_delay(Duration::from_millis(max_delay as u64));
        }
        if let Some(min_delay) = self.min_delay {
            retry = retry.with_min_delay(Duration::from_millis(min_delay as u64));
        }
        retry
    }
}

#[derive(ocaml::FromValue, ocaml::ToValue)]
#[ocaml::sig]
pub struct ImmutableIndexLayer {
    keys: Vec<String>,
}

impl ImmutableIndexLayer {
    pub fn build(self) -> od::layers::ImmutableIndexLayer {
        let mut layer = od::layers::ImmutableIndexLayer::default();
        for key in self.keys {
            layer.insert(key)
        }
        layer
    }
}

#[derive(ocaml::FromValue, ocaml::ToValue)]
#[ocaml::sig]
pub struct ConcurrentLimitLayer {
    permits: usize,
}

impl ConcurrentLimitLayer {
    pub fn build(self) -> od::layers::ConcurrentLimitLayer {
        od::layers::ConcurrentLimitLayer::new(self.permits)
    }
}

#[derive(ocaml::FromValue, ocaml::ToValue)]
#[ocaml::sig]
pub struct TimeoutLayer {
    timeout: Option<f64>,
    speed: Option<u64>,
}

impl TimeoutLayer {
    pub fn build(self) -> od::layers::TimeoutLayer {
        let mut layer = od::layers::TimeoutLayer::default();
        if let Some(timeout) = self.timeout {
            layer = layer.with_timeout(Duration::from_millis(timeout as u64));
        }
        if let Some(speed) = self.speed {
            layer = layer.with_speed(speed);
        }
        layer
    }
}

#[derive(ocaml::FromValue, ocaml::ToValue)]
#[ocaml::sig(
    "
| ConcurrentLimit of concurrent_limit_layer
| ImmutableIndex of immutable_index_layer
| Retry of retry_layer
| Timeout of timeout_layer
"
)]
pub enum Layer {
    ConcurrentLimit(ConcurrentLimitLayer),
    ImmutableIndex(ImmutableIndexLayer),
    Retry(RetryLayer),
    Timeout(TimeoutLayer),
}

#[ocaml::func]
#[ocaml::sig("int option -> float option -> bool -> float option -> float option -> layer ")]
pub fn new_retry_layer(
    max_times: Option<usize>,
    factor: Option<f32>,
    jitter: bool,
    max_delay: Option<f64>,
    min_delay: Option<f64>,
) -> Layer {
    Layer::Retry(RetryLayer {
        max_times,
        factor,
        jitter,
        max_delay,
        min_delay,
    })
}

#[ocaml::func]
#[ocaml::sig("string array -> layer ")]
pub fn new_immutable_index_layer(keys: Vec<String>) -> Layer {
    Layer::ImmutableIndex(ImmutableIndexLayer { keys })
}

#[ocaml::func]
#[ocaml::sig("int -> layer ")]
pub fn new_concurrent_limit_layer(permits: usize) -> Layer {
    Layer::ConcurrentLimit(ConcurrentLimitLayer { permits })
}

#[ocaml::func]
#[ocaml::sig("float option -> int64 option -> layer ")]
pub fn new_timeout_layer(timeout: Option<f64>, speed: Option<u64>) -> Layer {
    Layer::Timeout(TimeoutLayer { timeout, speed })
}
