use napi::bindgen_prelude::*;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use std::usize;

/// Layer is the mechanism to intercept operations.
#[derive(PartialEq, Eq)]
#[napi]
pub enum Layer {
    /// Add concurrent request limit.
    ///
    /// Users can control how many concurrent connections could be established between OpenDAL and underlying storage services.
    ///
    /// - @param {number} permits
    ///
    /// Create a new ConcurrentLimitLayer will specify permits
    ConcurrentLimit,

    /// Add an immutable in-memory index for underlying storage services.
    ///
    /// Especially useful for services without list capability like HTTP.
    ///
    /// - @param {Array<string>} keys
    ///
    /// The keys which will be inserted into the index.
    ///
    /// Please use `JSON.stringfy` to serialize because of type restrictions.
    ImmutableIndex,

    /// Add retry for temporary failed operations.
    ///
    /// - @param {boolean} jitter
    ///
    /// Set jitter of current backoff.
    ///
    /// If jitter is enabled, ExponentialBackoff will add a random jitter in `[0, min_delay)` to current delay.
    ///
    /// - @param {number} factor
    ///
    /// Set factor of current backoff.
    ///
    /// Panics!
    ///
    /// This function will panic if input factor smaller than 1.0.
    ///
    /// - @param {number} min_delay (unit: millisecond)
    ///
    /// Set min_delay of current backoff.
    ///
    /// - @param {number} max_delay  (unit: millisecond)
    ///
    /// Set max_delay of current backoff.
    ///
    /// Delay will not increasing if current delay is larger than max_delay.
    ///
    /// - @param {number} max_times
    ///
    /// Set max_times of current backoff.
    ///
    /// Backoff will return None if max times is reaching.
    Retry,
}

#[napi(object)]
pub struct LayerBuilder {
    pub layer_type: Layer,
    pub options: Option<HashMap<String, String>>,
}

pub struct ConcurrentLimitLayer(pub opendal::layers::ConcurrentLimitLayer);

impl ConcurrentLimitLayer {
    pub fn new(options: HashMap<String, String>) -> Result<Self> {
        let permits = options.get("permits").map(|s| usize::from_str(s));
        match permits {
            Some(Ok(permits)) => Ok(Self(opendal::layers::ConcurrentLimitLayer::new(permits))),
            _ => Err(Error::from_reason(
                "ConcurrentLimitLayer get invalid permits",
            )),
        }
    }
}

pub struct ImmutableIndexLayer(pub opendal::layers::ImmutableIndexLayer);

impl ImmutableIndexLayer {
    pub fn new(options: HashMap<String, String>) -> Result<Self> {
        let mut layer = opendal::layers::ImmutableIndexLayer::default();
        let s = options.get("keys");
        if let Some(s) = s {
            let t = json::parse(s).map_err(|e| Error::from_reason(e.to_string()))?;
            for i in 0..t.len() {
                if t[i].is_string() {
                    if let Some(s) = t[i].as_str() {
                        layer.insert(s.to_owned());
                    }
                } else {
                    return Err(Error::from_reason("ImmutableIndexLayer get invalid key"));
                }
            }
        }

        Ok(Self(layer))
    }

    pub fn insert(&mut self, key: String) {
        self.0.insert(key)
    }
}

pub struct RetryLayer(pub opendal::layers::RetryLayer);

impl RetryLayer {
    pub fn new(options: HashMap<String, String>) -> Result<Self> {
        let mut retry = opendal::layers::RetryLayer::default();

        if let Some(Ok(jitter)) = options.get("jitter").map(|s| bool::from_str(s)) {
            if jitter {
                retry = retry.with_jitter();
            }
        }

        if let Some(Ok(factor)) = options.get("factor").map(|s| f32::from_str(s)) {
            retry = retry.with_factor(factor);
        }

        if let Some(Ok(min_delay)) = options.get("min_delay").map(|s| u64::from_str(s)) {
            retry = retry.with_min_delay(Duration::from_micros(min_delay));
        }

        if let Some(Ok(max_delay)) = options.get("max_delay").map(|s| u64::from_str(s)) {
            retry = retry.with_max_delay(Duration::from_micros(max_delay));
        }

        if let Some(Ok(max_times)) = options.get("max_times").map(|s| usize::from_str(s)) {
            retry = retry.with_max_times(max_times);
        }

        Ok(Self(retry))
    }
}
