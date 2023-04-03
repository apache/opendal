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

import io.questdb.jar.jni.JarJniLoader;

import java.util.Map;


public class Operator {

    long ptr;

    public Operator(String schema, Map<String, String> params) {
        this.ptr = getOperator(schema, params);
    }

    public static final String ORG_APACHE_OPENDAL_RUST_LIBS = "/org/apache/opendal/rust/libs";

    public static final String OPENDAL_JAVA = "opendal_java";

    static {
        JarJniLoader.loadLib(
                Operator.class,
                ORG_APACHE_OPENDAL_RUST_LIBS,
                OPENDAL_JAVA);
    }

    private native long getOperator(String type, Map<String, String> params);

    private native void freeOperator(long ptr);

    private native void write(long ptr, String fileName, String content);

    private native String read(long ptr, String fileName);

    private native void delete(long ptr, String fileName);


    public void write(String fileName, String content) {
        write(this.ptr, fileName, content);
    }

    public String read(String s) {
        return read(this.ptr, s);
    }

    public void delete(String s) {
        delete(this.ptr, s);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.freeOperator(ptr);
    }
}
