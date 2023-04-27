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

public class Metadata extends OpenDALObject {
    public Metadata(long ptr) {
        this.ptr = ptr;
    }

    public boolean isFile() {
        return isFile(this.ptr);
    }

    public long getContentLength() {
        return getContentLength(this.ptr);
    }

    @Override
    public void close() {
        freeMetadata(this.ptr);
    }

    private native void freeMetadata(long statPtr);

    private native boolean isFile(long statPtr);

    private native long getContentLength(long statPtr);
}
