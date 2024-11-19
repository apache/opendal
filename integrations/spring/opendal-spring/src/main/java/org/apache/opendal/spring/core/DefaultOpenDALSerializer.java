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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.opendal.OpenDALException;

public class DefaultOpenDALSerializer<T> implements OpenDALSerializer<T> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final Class<T> clazz;

    public DefaultOpenDALSerializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    public static <T> DefaultOpenDALSerializer<T> of(Class<T> clazz) {
        return new DefaultOpenDALSerializer<>(clazz);
    }

    @Override
    public byte[] serialize(T t) throws OpenDALException {
        try {
            return MAPPER.writeValueAsBytes(t);
        } catch (Exception e) {
            throw new OpenDALException(OpenDALException.Code.Unexpected, e.toString());
        }
    }

    @Override
    public T deserialize(byte[] bytes) throws OpenDALException {
        try {
            return MAPPER.readValue(bytes, clazz);
        } catch (Exception e) {
            throw new OpenDALException(OpenDALException.Code.Unexpected, e.toString());
        }
    }
}
