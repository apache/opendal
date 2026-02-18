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

import java.time.Duration;
import org.apache.opendal.Layer;

/**
 * This layer adds timeout for every operation to avoid slow or unexpected hang operations.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.TimeoutLayer.html">TimeoutLayer's rustdoc</a>
 */
public class TimeoutLayer extends Layer {

    private final Duration timeout;

    private final Duration ioTimeout;

    /**
     * Create a new TimeoutLayer with default settings.
     * Default timeout: 60 seconds for non-IO operations, 10 seconds for IO operations.
     */
    public TimeoutLayer() {
        this.timeout = Duration.ofSeconds(60);
        this.ioTimeout = Duration.ofSeconds(10);
    }

    /**
     * Create a new TimeoutLayer with custom timeout settings.
     *
     * @param timeout timeout for non-IO operations (stat, delete, etc.)
     * @param ioTimeout timeout for IO operations (read, write, etc.)
     */
    public TimeoutLayer(Duration timeout, Duration ioTimeout) {
        this.timeout = timeout;
        this.ioTimeout = ioTimeout;
    }

    /**
     * Set timeout for non-IO operations.
     *
     * @param timeout the timeout duration
     * @return this TimeoutLayer for chaining
     */
    public TimeoutLayer withTimeout(Duration timeout) {
        return new TimeoutLayer(timeout, this.ioTimeout);
    }

    /**
     * Set timeout for IO operations.
     *
     * @param ioTimeout the IO timeout duration
     * @return this TimeoutLayer for chaining
     */
    public TimeoutLayer withIoTimeout(Duration ioTimeout) {
        return new TimeoutLayer(this.timeout, ioTimeout);
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, timeout.toNanos(), ioTimeout.toNanos());
    }

    private static native long doLayer(long nativeHandle, long timeout, long ioTimeout);
}
