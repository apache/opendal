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

package org.apache.opendal.spring.core;

import org.apache.opendal.AsyncOperator;

import java.util.concurrent.CompletableFuture;

public class OpenDALOperationsImpl<T> implements OpenDALOperations<T> {
    private final AsyncOperator asyncOperator;

    private final OpenDALSerializer<T> serializer;

    public OpenDALOperationsImpl(AsyncOperator asyncOperator, OpenDALSerializer<T> serializer) {
        this.asyncOperator = asyncOperator;
        this.serializer = serializer;
    }

    @Override
    public void write(String path, T entity) {
        byte[] bytes = serializer.serialize(entity);
        asyncOperator.blocking().write(path, bytes);
    }

    @Override
    public T read(String path) {
        byte[] bytes = asyncOperator.blocking().read(path);
        return serializer.deserialize(bytes);
    }

    @Override
    public void delete(String path) {
        asyncOperator.blocking().delete(path);
    }

    @Override
    public CompletableFuture<Void> writeAsync(String path, T entity) {
        byte[] bytes = serializer.serialize(entity);
        return asyncOperator.write(path, bytes);
    }

    @Override
    public CompletableFuture<T> readAsync(String path) {
        CompletableFuture<byte[]> bytes = asyncOperator.read(path);
        return bytes.thenApply(serializer::deserialize);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        return asyncOperator.delete(path);
    }
}
