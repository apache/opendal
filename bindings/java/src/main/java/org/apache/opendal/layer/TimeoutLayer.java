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
 * Applies deadlines to control operations and individual I/O calls.
 * When a deadline expires, the layer cancels the in-flight call and reports an OpenDAL error.
 * Add this layer before {@link RetryLayer} to apply the deadline to each attempt.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.TimeoutLayer.html">TimeoutLayer's rustdoc</a>
 */
public class TimeoutLayer extends Layer {
    private static final Duration MAX_TIMEOUT = Duration.ofNanos(Long.MAX_VALUE);

    private final Duration timeout;
    private final Duration ioTimeout;

    /**
     * Creates a layer with a 60-second timeout for control operations and a 10-second timeout for I/O calls.
     */
    public TimeoutLayer() {
        this(Duration.ofSeconds(60), Duration.ofSeconds(10));
    }

    /**
     * Creates a layer with the supplied timeouts.
     *
     * @param timeout the timeout for control operations such as stat
     * @param ioTimeout the timeout for individual I/O calls such as reading or writing a buffer
     * @throws IllegalArgumentException if either duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
     */
    public TimeoutLayer(Duration timeout, Duration ioTimeout) {
        if (timeout.isNegative() || timeout.compareTo(MAX_TIMEOUT) > 0) {
            throw new IllegalArgumentException("timeout must be between zero and Long.MAX_VALUE nanoseconds");
        }
        if (ioTimeout.isNegative() || ioTimeout.compareTo(MAX_TIMEOUT) > 0) {
            throw new IllegalArgumentException("ioTimeout must be between zero and Long.MAX_VALUE nanoseconds");
        }
        this.timeout = timeout;
        this.ioTimeout = ioTimeout;
    }

    /**
     * Returns a new layer with the specified control operation timeout.
     *
     * @param timeout the timeout for control operations
     * @return a new layer with the same I/O timeout
     * @throws IllegalArgumentException if the duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
     */
    public TimeoutLayer withTimeout(Duration timeout) {
        return new TimeoutLayer(timeout, ioTimeout);
    }

    /**
     * Returns a new layer with the specified I/O timeout.
     *
     * @param ioTimeout the timeout for individual I/O calls
     * @return a new layer with the same control operation timeout
     * @throws IllegalArgumentException if the duration is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
     */
    public TimeoutLayer withIoTimeout(Duration ioTimeout) {
        return new TimeoutLayer(timeout, ioTimeout);
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, timeout.toNanos(), ioTimeout.toNanos());
    }

    private static native long doLayer(long nativeHandle, long timeoutNanos, long ioTimeoutNanos);
}
