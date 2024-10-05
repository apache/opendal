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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReactiveOpenDALTemplate {
    private final AsyncOperator asyncOperator;

    private final OpenDALSerializerFactory serializerFactory;

    private final Map<Class<?>, ReactiveOpenDALOperations<?>> operationsMap = new ConcurrentHashMap<>();

    public ReactiveOpenDALTemplate(AsyncOperator asyncOperator, OpenDALSerializerFactory serializerFactory) {
        this.asyncOperator = asyncOperator;
        this.serializerFactory = serializerFactory;
    }

    @SuppressWarnings("unchecked")
    public <T> ReactiveOpenDALOperations<T> ops(Class<T> clazz) {
        return (ReactiveOpenDALOperations<T>) operationsMap.computeIfAbsent(clazz, (k) -> {
            OpenDALSerializer<T> serializer = (OpenDALSerializer<T>) serializerFactory.getSerializer(
                k);
            return new ReactiveOpenDALOperationsImpl<>(asyncOperator, serializer);
        });
    }
}
