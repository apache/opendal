// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::Layer;
use super::PythonLayer;
use crate::*;
use opendal::Operator;

/// A layer that configures the operator's HTTP transport.
///
/// The layer replaces the operator's HTTP client with one built from the given
/// options, so it applies to that operator only: two operators in the same
/// process can use different proxies, timeouts, and user agents.
///
/// Notes
/// -----
/// Setting ``proxy`` replaces the proxy environment variables
/// (``HTTP_PROXY``, ``HTTPS_PROXY``, ``ALL_PROXY``, and ``NO_PROXY``) for this
/// operator. Use ``no_proxy`` to exclude hosts from the proxy.
///
/// The layer replaces the operator's transport instead of wrapping it. Apply it
/// before layers that wrap the transport, such as ``ConcurrentLimitLayer``.
#[pyclass(module = "opendal.layers", extends = Layer, skip_from_py_object)]
#[derive(Clone)]
pub struct HttpConfigLayer(ocore::layers::HttpConfigLayer);

impl PythonLayer for HttpConfigLayer {
    fn layer(&self, op: Operator) -> Operator {
        op.layer(self.0.clone())
    }
}
#[pymethods]
impl HttpConfigLayer {
    /// Create a new HttpConfigLayer.
    ///
    /// Parameters
    /// ----------
    /// proxy : Optional[str]
    ///     Proxy URL used for all requests from this operator, for example
    ///     ``http://127.0.0.1:7890``. The URL may carry credentials.
    /// no_proxy : Optional[str]
    ///     Comma-separated hosts and domains that bypass ``proxy``, for
    ///     example ``169.254.169.254,.internal.example.com``. Requires
    ///     ``proxy``.
    /// connect_timeout : Optional[datetime.timedelta]
    ///     Timeout for establishing a connection. Must not be negative.
    /// read_timeout : Optional[datetime.timedelta]
    ///     Timeout for a single read from the connection. Must not be
    ///     negative.
    /// timeout : Optional[datetime.timedelta]
    ///     Timeout for a whole HTTP request, including connecting, sending
    ///     the request, and reading the response body. Must not be negative.
    /// user_agent : Optional[str]
    ///     ``User-Agent`` header sent with every request from this operator.
    ///
    /// Returns
    /// -------
    /// HttpConfigLayer
    ///
    /// Raises
    /// ------
    /// ConfigInvalid
    ///     If ``no_proxy`` is set without ``proxy``, a duration is negative,
    ///     or a proxy URL is invalid.
    /// Unsupported
    ///     If the installed HTTP transport cannot apply these options.
    #[new]
    #[pyo3(signature = (
        proxy = None,
        no_proxy = None,
        connect_timeout = None,
        read_timeout = None,
        timeout = None,
        user_agent = None,
    ))]
    fn new(
        proxy: Option<String>,
        no_proxy: Option<String>,
        connect_timeout: Option<ocore::raw::SignedDuration>,
        read_timeout: Option<ocore::raw::SignedDuration>,
        timeout: Option<ocore::raw::SignedDuration>,
        user_agent: Option<String>,
    ) -> PyResult<PyClassInitializer<Self>> {
        let config = ocore::HttpConfig {
            proxy,
            no_proxy,
            connect_timeout,
            read_timeout,
            timeout,
            user_agent,
        };
        let http_config = Self(ocore::layers::HttpConfigLayer::new(config).map_err(format_pyerr)?);
        let class = PyClassInitializer::from(Layer(Box::new(http_config.clone())))
            .add_subclass(http_config);

        Ok(class)
    }
}
