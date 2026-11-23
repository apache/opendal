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
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Layer;
import org.apache.opendal.OpenDALException;

/**
 * Cancels slow calls using an adaptive deadline based on recent successful calls.
 * Once enough samples are available, the layer multiplies the configured latency percentile by the
 * safety factor and clamps the result to the configured deadline range. A cancelled call returns a
 * temporary {@link OpenDALException} with code {@link OpenDALException.Code#Unexpected}.
 *
 * <p>Each call to {@link AsyncOperator#layer(Layer)} with this layer creates an independent latency
 * history. Use {@link AsyncOperator#duplicate()} on the returned handle to share latency statistics
 * across handles. Add this layer before {@link RetryLayer} so retries can handle cancellations.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.TailCutLayer.html">TailCutLayer's rustdoc</a>
 */
public class TailCutLayer extends Layer {
    private static final Duration MAX_WINDOW = Duration.ofSeconds(120);
    private static final Duration MAX_DEADLINE = Duration.ofNanos(Long.MAX_VALUE);

    private final int percentile;
    private final double safetyFactor;
    private final long windowNanos;
    private final int minSamples;
    private final long minDeadlineNanos;
    private final long maxDeadlineNanos;

    /**
     * Creates a layer with percentile 95, a safety factor of 1.3, a 60-second window, 200 minimum
     * samples, and deadlines between 500 milliseconds and 30 seconds.
     */
    public TailCutLayer() {
        this(builder());
    }

    private TailCutLayer(TailCutLayerBuilder builder) {
        this.percentile = builder.percentile;
        this.safetyFactor = builder.safetyFactor;
        this.windowNanos = builder.window.toNanos();
        this.minSamples = builder.minSamples;
        this.minDeadlineNanos = builder.minDeadline.toNanos();
        this.maxDeadlineNanos = builder.maxDeadline.toNanos();
    }

    /**
     * Returns a builder initialized with the default settings.
     *
     * @return a new builder
     */
    public static TailCutLayerBuilder builder() {
        return new TailCutLayerBuilder();
    }

    /** Configures the latency history and adaptive deadline. */
    public static class TailCutLayerBuilder {
        private int percentile = 95;
        private double safetyFactor = 1.3;
        private Duration window = Duration.ofSeconds(60);
        private int minSamples = 200;
        private Duration minDeadline = Duration.ofMillis(500);
        private Duration maxDeadline = Duration.ofSeconds(30);

        private TailCutLayerBuilder() {}

        /**
         * Sets the latency percentile used to calculate the deadline.
         *
         * @param percentile the percentile, from 50 to 99 inclusive
         * @return this builder
         * @throws IllegalArgumentException if the percentile is outside the allowed range
         */
        public TailCutLayerBuilder percentile(int percentile) {
            if (percentile < 50 || percentile > 99) {
                throw new IllegalArgumentException("percentile must be between 50 and 99");
            }
            this.percentile = percentile;
            return this;
        }

        /**
         * Sets the multiplier applied to the observed latency percentile.
         *
         * @param safetyFactor the multiplier, from 1.0 to 5.0 inclusive
         * @return this builder
         * @throws IllegalArgumentException if the multiplier is NaN or outside the allowed range
         */
        public TailCutLayerBuilder safetyFactor(double safetyFactor) {
            if (Double.isNaN(safetyFactor) || safetyFactor < 1.0 || safetyFactor > 5.0) {
                throw new IllegalArgumentException("safetyFactor must be between 1.0 and 5.0");
            }
            this.safetyFactor = safetyFactor;
            return this;
        }

        /**
         * Sets the sliding window for collecting latency statistics.
         *
         * @param window the window duration
         * @return this builder
         * @throws IllegalArgumentException if the window is null, negative, or longer than 120 seconds
         */
        public TailCutLayerBuilder window(Duration window) {
            if (window == null || window.isNegative() || window.compareTo(MAX_WINDOW) > 0) {
                throw new IllegalArgumentException("window must be between zero and 120 seconds");
            }
            this.window = window;
            return this;
        }

        /**
         * Sets the minimum number of latency samples required to enable cancellation.
         *
         * @param minSamples the minimum number of samples
         * @return this builder
         * @throws IllegalArgumentException if the minimum is negative
         */
        public TailCutLayerBuilder minSamples(int minSamples) {
            if (minSamples < 0) {
                throw new IllegalArgumentException("minSamples must be non-negative");
            }
            this.minSamples = minSamples;
            return this;
        }

        /**
         * Sets the lower bound for the adaptive deadline.
         *
         * @param minDeadline the minimum deadline
         * @return this builder
         * @throws IllegalArgumentException if the deadline is null, negative, or exceeds {@link Long#MAX_VALUE} nanoseconds
         */
        public TailCutLayerBuilder minDeadline(Duration minDeadline) {
            this.minDeadline = validateDeadline(minDeadline, "minDeadline");
            return this;
        }

        /**
         * Sets the upper bound for the adaptive deadline.
         *
         * @param maxDeadline the maximum deadline
         * @return this builder
         * @throws IllegalArgumentException if the deadline is null, negative, or exceeds {@link Long#MAX_VALUE} nanoseconds
         */
        public TailCutLayerBuilder maxDeadline(Duration maxDeadline) {
            this.maxDeadline = validateDeadline(maxDeadline, "maxDeadline");
            return this;
        }

        /**
         * Builds a tail-cut layer with the configured settings.
         *
         * @return a new tail-cut layer
         * @throws IllegalArgumentException if the minimum deadline exceeds the maximum deadline
         */
        public TailCutLayer build() {
            if (minDeadline.compareTo(maxDeadline) > 0) {
                throw new IllegalArgumentException("minDeadline must be less than or equal to maxDeadline");
            }
            return new TailCutLayer(this);
        }

        private static Duration validateDeadline(Duration deadline, String name) {
            if (deadline == null || deadline.isNegative() || deadline.compareTo(MAX_DEADLINE) > 0) {
                throw new IllegalArgumentException(name + " must be between zero and Long.MAX_VALUE nanoseconds");
            }
            return deadline;
        }
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, percentile, safetyFactor, windowNanos, minSamples, minDeadlineNanos, maxDeadlineNanos);
    }

    private static native long doLayer(
            long nativeHandle,
            int percentile,
            double safetyFactor,
            long windowNanos,
            int minSamples,
            long minDeadlineNanos,
            long maxDeadlineNanos);
}
