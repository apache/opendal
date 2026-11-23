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
import lombok.Builder;
import org.apache.opendal.Layer;

/**
 * This layer will retry failed operations when {@code Error::is_temporary} returns {@code true}.
 * If operation still failed, this layer will set error to Persistent which means error has been retried.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.RetryLayer.html">RetryLayer's rustdoc</a>
 */
@Builder
public class RetryLayer extends Layer {
    private static final Duration MAX_DELAY = Duration.ofNanos(Long.MAX_VALUE);

    private final boolean jitter;

    @Builder.Default
    private final float factor = 2;

    @Builder.Default
    private final Duration minDelay = Duration.ofSeconds(1);

    @Builder.Default
    private final Duration maxDelay = Duration.ofSeconds(60);

    @Builder.Default
    private final long maxTimes = 3;

    /**
     * Creates a retry layer.
     *
     * @param jitter whether each delay is randomized
     * @param factor the multiplier applied to the delay after each failure
     * @param minDelay the delay before the first retry
     * @param maxDelay the upper bound of the delay between retries
     * @param maxTimes the maximum number of retries
     * @throws IllegalArgumentException if a delay is negative or exceeds {@link Long#MAX_VALUE} nanoseconds
     */
    RetryLayer(boolean jitter, float factor, Duration minDelay, Duration maxDelay, long maxTimes) {
        if (minDelay.isNegative() || minDelay.compareTo(MAX_DELAY) > 0) {
            throw new IllegalArgumentException("minDelay must be between zero and Long.MAX_VALUE nanoseconds");
        }
        if (maxDelay.isNegative() || maxDelay.compareTo(MAX_DELAY) > 0) {
            throw new IllegalArgumentException("maxDelay must be between zero and Long.MAX_VALUE nanoseconds");
        }
        this.jitter = jitter;
        this.factor = factor;
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
        this.maxTimes = maxTimes;
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, jitter, factor, minDelay.toNanos(), maxDelay.toNanos(), maxTimes);
    }

    private static native long doLayer(
            long nativeHandle, boolean jitter, float factor, long minDelay, long maxDelay, long maxTimes);
}
