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

import java.util.Map;

public class BlockingOperator extends NativeObject {
    public BlockingOperator(String schema, Map<String, String> map) {
        super(constructor(schema, map));
    }

    public void write(String fileName, String content) {
        write(nativeHandle, fileName, content);
    }

    public String read(String s) {
        return read(nativeHandle, s);
    }

    public void delete(String s) {
        delete(nativeHandle, s);
    }

    public Metadata stat(String fileName) {
        return new Metadata(stat(nativeHandle, fileName));
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long constructor(String schema, Map<String, String> map);
    private static native void write(long nativeHandle, String fileName, String content);
    private static native String read(long nativeHandle, String fileName);
    private static native void delete(long nativeHandle, String fileName);
    private static native long stat(long nativeHandle, String file);
}
