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

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * BlockingOperator represents an underneath OpenDAL operator that
 * accesses data synchronously.
 */
public class BlockingOperator extends NativeObject {
    /**
     * Construct an OpenDAL blocking operator:
     *
     * <p>
     * You can find all possible schemes <a href="https://docs.rs/opendal/latest/opendal/enum.Scheme.html">here</a>
     * and see what config options each service supports.
     *
     * @param schema the name of the underneath service to access data from.
     * @param map a map of properties to construct the underneath operator.
     */
    public BlockingOperator(String schema, Map<String, String> map) {
        super(constructor(schema, map));
    }

    public void write(String path, String content) {
        write(path, content.getBytes(StandardCharsets.UTF_8));
    }

    public void write(String path, byte[] content) {
        write(nativeHandle, path, content);
    }

    public String read(String path) {
        return read(nativeHandle, path);
    }

    public void delete(String path) {
        delete(nativeHandle, path);
    }

    public Metadata stat(String path) {
        return new Metadata(stat(nativeHandle, path));
    }

    public OperatorInfo info() {
        return info(nativeHandle);
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long constructor(String schema, Map<String, String> map);

    private static native void write(long nativeHandle, String path, byte[] content);

    private static native String read(long nativeHandle, String path);

    private static native void delete(long nativeHandle, String path);

    private static native long stat(long nativeHandle, String path);

    private static native OperatorInfo info(long nativeHandle);
}
