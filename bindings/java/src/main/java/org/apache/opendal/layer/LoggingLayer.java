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
 * This layer adds structured logging for every operation.
 *
 * <p>Note: This layer requires proper logging setup (e.g., log4j2, java.util.logging) to work.
 * Without logging configuration, this layer is a no-op.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html">LoggingLayer's rustdoc</a>
 */
public class LoggingLayer extends Layer {

    /**
     * Create a new LoggingLayer.
     */
    public LoggingLayer() {}

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp);
    }

    private static native long doLayer(long nativeHandle);
}
