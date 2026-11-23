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
 * Injects temporary {@link org.apache.opendal.OpenDALException.Code#Unexpected} errors when opening
 * a read or reading data, to exercise error handling in tests.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.ChaosLayer.html">ChaosLayer's rustdoc</a>
 */
public class ChaosLayer extends Layer {

    private final double errorRatio;

    /**
     * Creates a layer with the supplied error ratio.
     *
     * @param errorRatio the chance that a read call fails, from 0.0 (none) to 1.0 (all),
     *     rounded down to a whole percentage point
     * @throws IllegalArgumentException if the ratio is outside [0.0, 1.0] or is NaN
     */
    public ChaosLayer(double errorRatio) {
        if (!(errorRatio >= 0.0 && errorRatio <= 1.0)) {
            throw new IllegalArgumentException("errorRatio must be between 0.0 and 1.0");
        }
        this.errorRatio = errorRatio;
    }

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp, errorRatio);
    }

    private static native long doLayer(long nativeHandle, double errorRatio);
}
