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

    private final boolean jitter;

    @Builder.Default
    private final float factor = 2;

    @Builder.Default
    private final Duration minDelay = Duration.ofSeconds(1);

    @Builder.Default
    private final Duration maxDelay = Duration.ofSeconds(60);

    @Builder.Default
    private final long maxTimes = 3;

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, jitter, factor, minDelay.toNanos(), maxDelay.toNanos(), maxTimes);
    }

    private static native long doLayer(
            long nativeHandle, boolean jitter, float factor, long minDelay, long maxDelay, long maxTimes);
}
