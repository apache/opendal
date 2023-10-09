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
    public final OperatorInfo info;

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
    public static BlockingOperator of(String schema, Map<String, String> map) {
        try (final Operator operator = Operator.of(schema, map)) {
            return operator.blocking();
        }
    }

    BlockingOperator(long nativeHandle, OperatorInfo info) {
        super(nativeHandle);
        this.info = info;
    }

    public void write(String path, String content) {
        write(path, content.getBytes(StandardCharsets.UTF_8));
    }

    public void write(String path, byte[] content) {
        write(nativeHandle, path, content);
    }

    public byte[] read(String path) {
        return read(nativeHandle, path);
    }

    public void delete(String path) {
        delete(nativeHandle, path);
    }

    public Metadata stat(String path) {
        return new Metadata(stat(nativeHandle, path));
    }

    public void createDir(String path) {
        createDir(nativeHandle, path);
    }

    public void copy(String sourcePath, String targetPath) {
        copy(nativeHandle, sourcePath, targetPath);
    }

    public void rename(String sourcePath, String targetPath) {
        rename(nativeHandle, sourcePath, targetPath);
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native void write(long nativeHandle, String path, byte[] content);

    private static native byte[] read(long nativeHandle, String path);

    private static native void delete(long nativeHandle, String path);

    private static native long stat(long nativeHandle, String path);

    private static native long createDir(long nativeHandle, String path);

    private static native long copy(long nativeHandle, String sourcePath, String targetPath);

    private static native long rename(long nativeHandle, String sourcePath, String targetPath);
}
