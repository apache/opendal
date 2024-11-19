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
import reactor.core.publisher.Mono;

public class ReactiveOpenDALOperationsImpl<T> implements ReactiveOpenDALOperations<T> {
    private final AsyncOperator asyncOperator;

    private final OpenDALSerializer<T> serializer;

    public ReactiveOpenDALOperationsImpl(AsyncOperator asyncOperator, OpenDALSerializer<T> serializer) {
        this.asyncOperator = asyncOperator;
        this.serializer = serializer;
    }

    @Override
    public Mono<Void> write(String path, T entity) {
        return Mono.fromFuture(asyncOperator.write(path, serializer.serialize(entity)));
    }

    @Override
    public Mono<T> read(String path) {
        return Mono.fromFuture(asyncOperator.read(path)).map(serializer::deserialize);
    }

    @Override
    public Mono<Void> delete(String path) {
        return Mono.fromFuture(asyncOperator.delete(path));
    }
}
