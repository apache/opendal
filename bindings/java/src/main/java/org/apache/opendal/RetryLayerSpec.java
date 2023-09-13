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

package org.apache.opendal;

import java.time.Duration;
import lombok.Builder;

@Builder
public final class RetryLayerSpec implements LayerSpec {
    /**
     * Backoff will return None if max times is reaching.
     */
    private final int maxTimes;

    /**
     * Backoff factor.
     */
    private final float factor;

    /**
     * If jitter is enabled, ExponentialBackoff will add a random jitter in [0, min_delay) to current delay.
     */
    private final boolean jitter;

    /**
     * Delay will not increase if current delay is larger than maxDelay.
     */
    private final Duration maxDelay;

    /**
     * Backoff minimal delay.
     */
    private final Duration minDelay;

    @SuppressWarnings("unused") // referred by lombok generated code
    public static final class RetryLayerSpecBuilder {
        public RetryLayerSpecBuilder() {
            this.maxTimes = -1;
        }

        public RetryLayerSpecBuilder maxTimes(int maxTimes) {
            if (maxTimes < 0) {
                throw new IllegalArgumentException(String.format("maxTimes (%d) must >= 0", maxTimes));
            }
            this.maxTimes = maxTimes;
            return this;
        }

        public RetryLayerSpecBuilder factor(float factor) {
            if (factor < 1.0) {
                throw new IllegalArgumentException(String.format("factor (%f) must >= 1.0", factor));
            }
            this.factor = factor;
            return this;
        }

        public RetryLayerSpec build() {
            return new RetryLayerSpec(maxTimes, factor, jitter, maxDelay, minDelay);
        }
    }

    /**
     * This method is called from native code.
     *
     * @return pointer of the constructed Layer.
     */
    @SuppressWarnings("unused")
    private long constructLayer() {
        final long maxDelay = this.maxDelay != null ? this.maxDelay.toNanos() : -1;
        final long minDelay = this.minDelay != null ? this.minDelay.toNanos() : -1;
        return constructLayer0(maxTimes, factor, jitter, maxDelay, minDelay);
    }

    private static native long constructLayer0(
            int maxTimes, float factor, boolean jitter, long maxDelay, long minDelay);
}
