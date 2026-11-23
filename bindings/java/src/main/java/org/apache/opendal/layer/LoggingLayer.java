/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.opendal.layer;

import org.apache.opendal.Layer;

/**
 * Records OpenDAL operations and outbound HTTP requests through the Rust {@code log} facade.
 *
 * <p>Each operation logs a {@code started} entry and then a terminal status: {@code succeeded},
 * {@code finished}, or {@code failed}. Expected errors log at {@code Warn} and unexpected errors
 * log at {@code Error}. HTTP request starts and response statuses log at {@code Debug}; HTTP errors
 * log by kind at {@code Warn} or {@code Error}. HTTP records omit URLs, headers, bodies, and error
 * details.
 *
 * <p>This layer uses OpenDAL's default logging interceptor. The Java binding does not install a
 * Rust logger, so the layer produces no output unless the process provides one.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html">LoggingLayer's rustdoc</a>
 */
public class LoggingLayer extends Layer {

    /** Creates a logging layer with OpenDAL's default logging interceptor. */
    public LoggingLayer() {}

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp);
    }

    private static native long doLayer(long nativeHandle);
}
