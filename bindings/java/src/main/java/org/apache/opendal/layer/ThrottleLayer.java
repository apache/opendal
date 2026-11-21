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
 * Limits bandwidth for bounded read ranges and write buffers.
 * A range or buffer larger than the configured burst fails with
 * {@link org.apache.opendal.OpenDALException.Code#RateLimited}.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.ThrottleLayer.html">ThrottleLayer's rustdoc</a>
 */
public class ThrottleLayer extends Layer {
    private static final long MAX_VALUE = 0xFFFFFFFFL;

    private final long bandwidth;
    private final long burst;

    /**
     * Creates a bandwidth limiter.
     *
     * @param bandwidth the number of bytes allowed per second
     * @param burst the number of bytes allowed in a single burst
     * @throws IllegalArgumentException if either argument is outside 1 through 4,294,967,295 (inclusive)
     */
    public ThrottleLayer(long bandwidth, long burst) {
        if (bandwidth <= 0 || bandwidth > MAX_VALUE) {
            throw new IllegalArgumentException("bandwidth must be between 1 and " + MAX_VALUE);
        }
        if (burst <= 0 || burst > MAX_VALUE) {
            throw new IllegalArgumentException("burst must be between 1 and " + MAX_VALUE);
        }
        this.bandwidth = bandwidth;
        this.burst = burst;
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, bandwidth, burst);
    }

    private static native long doLayer(long nativeHandle, long bandwidth, long burst);
}
