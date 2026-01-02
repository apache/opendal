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
 * This layer will automatically set Content-Type based on the file extension in the path.
 *
 * @see <a href="https://docs.rs/opendal/latest/opendal/layers/struct.MimeGuessLayer.html">MimeGuessLayer's rustdoc</a>
 */
public class MimeGuessLayer extends Layer {

    /**
     * Create a new MimeGuessLayer.
     */
    public MimeGuessLayer() {}

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp);
    }

    private static native long doLayer(long nativeHandle);
}
