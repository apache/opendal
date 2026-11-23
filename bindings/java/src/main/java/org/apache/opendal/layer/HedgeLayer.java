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
 * Starts duplicate attempts for slow stat calls and reads.
 * By default, both operations can launch one additional attempt per request after five seconds.
 * Each additional attempt waits a full delay after the previous launch. The builder's {@code delay}
 * sets the shared delay, and {@code statDelay} and {@code readDelay} override it per operation.
 * Set {@code maxHedges} to zero to disable hedging, or set {@code hedgeStat} or {@code hedgeRead}
 * to false to disable hedging for that operation.
 *
 * <p>The first success or non-temporary error ends the race. A temporary error prevents further launches
 * and lets any attempts already in flight finish. If every attempt fails temporarily, the layer
 * returns the last observed error. On reads, the layer races range requests, including opening a
 * stream. The caller consumes the winning stream without hedging individual chunks.
 * When the race ends, the layer cancels outstanding attempts on the client. The service may still
 * process requests it already received.
 *
 * <p>Add this layer before {@link TimeoutLayer} and {@link RetryLayer} so that a timeout covers
 * the entire race and retries can handle temporary errors from the race.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.HedgeLayer.html">HedgeLayer's rustdoc</a>
 */
public class HedgeLayer extends Layer {
    private static final Duration MAX_DELAY = Duration.ofNanos(Long.MAX_VALUE);

    private final long statDelayNanos;
    private final long readDelayNanos;
    private final int maxHedges;
    private final boolean hedgeStat;
    private final boolean hedgeRead;

    @Builder
    private HedgeLayer(
            Duration delay,
            Duration statDelay,
            Duration readDelay,
            int maxHedges,
            boolean hedgeStat,
            boolean hedgeRead) {
        this.statDelayNanos = (statDelay == null ? delay : statDelay).toNanos();
        this.readDelayNanos = (readDelay == null ? delay : readDelay).toNanos();
        this.maxHedges = maxHedges;
        this.hedgeStat = hedgeStat;
        this.hedgeRead = hedgeRead;
    }

    private static Duration validateDelay(Duration delay, String name) {
        if (delay == null || delay.isNegative() || delay.compareTo(MAX_DELAY) > 0) {
            throw new IllegalArgumentException(name + " must be between zero and Long.MAX_VALUE nanoseconds");
        }
        return delay;
    }

    /** Builds a hedge layer with optional per-operation delays. */
    public static class HedgeLayerBuilder {
        private Duration delay = Duration.ofSeconds(5);
        private Duration statDelay;
        private Duration readDelay;
        private int maxHedges = 1;
        private boolean hedgeStat = true;
        private boolean hedgeRead = true;

        /**
         * Sets the shared delay between attempts. Zero requests the shortest delay.
         *
         * @param delay the shared delay
         * @return this builder
         * @throws IllegalArgumentException if the delay is null, negative, or exceeds {@link Long#MAX_VALUE} nanoseconds
         */
        public HedgeLayerBuilder delay(Duration delay) {
            this.delay = validateDelay(delay, "delay");
            return this;
        }

        /**
         * Overrides the delay between stat attempts.
         *
         * @param statDelay the stat delay
         * @return this builder
         * @throws IllegalArgumentException if the delay is null, negative, or exceeds {@link Long#MAX_VALUE} nanoseconds
         */
        public HedgeLayerBuilder statDelay(Duration statDelay) {
            this.statDelay = validateDelay(statDelay, "statDelay");
            return this;
        }

        /**
         * Overrides the delay between attempts to open or read a byte range.
         *
         * @param readDelay the read delay
         * @return this builder
         * @throws IllegalArgumentException if the delay is null, negative, or exceeds {@link Long#MAX_VALUE} nanoseconds
         */
        public HedgeLayerBuilder readDelay(Duration readDelay) {
            this.readDelay = validateDelay(readDelay, "readDelay");
            return this;
        }

        /**
         * Sets the maximum number of additional attempts per request. Zero disables hedging.
         *
         * @param maxHedges the maximum number of additional attempts
         * @return this builder
         * @throws IllegalArgumentException if the maximum is negative
         */
        public HedgeLayerBuilder maxHedges(int maxHedges) {
            if (maxHedges < 0) {
                throw new IllegalArgumentException("maxHedges must be non-negative");
            }
            this.maxHedges = maxHedges;
            return this;
        }
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, statDelayNanos, readDelayNanos, maxHedges, hedgeStat, hedgeRead);
    }

    private static native long doLayer(
            long nativeHandle,
            long statDelayNanos,
            long readDelayNanos,
            int maxHedges,
            boolean hedgeStat,
            boolean hedgeRead);
}
