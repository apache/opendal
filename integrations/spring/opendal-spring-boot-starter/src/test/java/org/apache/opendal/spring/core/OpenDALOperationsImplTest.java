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
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class OpenDALOperationsImplTest {
    private static final OpenDALSerializer<String> STRING_SERIALIZER = new OpenDALSerializer<>() {
        @Override
        public byte[] serialize(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    };

    @Test
    void blockingOperationsCloseAcquiredOperators() {
        List<Operator> acquired = new ArrayList<>();

        try (AsyncOperator realOperator = AsyncOperator.of("memory", Map.of())) {
            AsyncOperator trackingOperator = Mockito.mock(AsyncOperator.class);
            Mockito.when(trackingOperator.blocking()).thenAnswer(invocation -> {
                Operator blocking = realOperator.blocking();
                acquired.add(blocking);
                return blocking;
            });

            OpenDALOperations<String> ops = new OpenDALOperationsImpl<>(trackingOperator, STRING_SERIALIZER);

            ops.write("k", "hello");
            Assertions.assertEquals(1, acquired.size());
            Assertions.assertTrue(acquired.get(0).isDisposed());

            Assertions.assertEquals("hello", ops.read("k"));
            Assertions.assertEquals(2, acquired.size());
            Assertions.assertTrue(acquired.get(1).isDisposed());

            ops.delete("k");
            Assertions.assertEquals(3, acquired.size());
            Assertions.assertTrue(acquired.get(2).isDisposed());
        }
    }

    @Test
    void blockingReadClosesOperatorOnExceptionWithoutWrapping() {
        List<Operator> acquired = new ArrayList<>();

        try (AsyncOperator realOperator = AsyncOperator.of("memory", Map.of())) {
            AsyncOperator trackingOperator = Mockito.mock(AsyncOperator.class);
            Mockito.when(trackingOperator.blocking()).thenAnswer(invocation -> {
                Operator blocking = realOperator.blocking();
                acquired.add(blocking);
                return blocking;
            });

            OpenDALOperations<String> ops = new OpenDALOperationsImpl<>(trackingOperator, STRING_SERIALIZER);

            OpenDALException ex = Assertions.assertThrows(OpenDALException.class, () -> ops.read("missing"));
            Assertions.assertEquals(OpenDALException.Code.NotFound, ex.getCode());
            Assertions.assertEquals(1, acquired.size());
            Assertions.assertTrue(acquired.get(0).isDisposed());
        }
    }

    @Test
    void blockingWriteClosesOperatorOnException() {
        List<Operator> acquired = new ArrayList<>();

        try (AsyncOperator realOperator = AsyncOperator.of("memory", Map.of())) {
            AsyncOperator trackingOperator = Mockito.mock(AsyncOperator.class);
            Mockito.when(trackingOperator.blocking()).thenAnswer(invocation -> {
                Operator blocking = realOperator.blocking();
                acquired.add(blocking);
                return blocking;
            });

            OpenDALOperations<String> ops = new OpenDALOperationsImpl<>(trackingOperator, STRING_SERIALIZER);

            OpenDALException ex = Assertions.assertThrows(OpenDALException.class, () -> ops.write("dir/", "bad"));
            Assertions.assertEquals(OpenDALException.Code.IsADirectory, ex.getCode());
            Assertions.assertEquals(1, acquired.size());
            Assertions.assertTrue(acquired.get(0).isDisposed());
        }
    }

    @Test
    void blockingDeleteClosesOperatorOnException() {
        Operator blockingOperator = Mockito.mock(Operator.class);
        Mockito.doThrow(new IllegalStateException("delete failed")).when(blockingOperator).delete("fail");

        AsyncOperator asyncOperator = Mockito.mock(AsyncOperator.class);
        Mockito.when(asyncOperator.blocking()).thenReturn(blockingOperator);

        OpenDALOperations<String> ops = new OpenDALOperationsImpl<>(asyncOperator, STRING_SERIALIZER);

        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class, () -> ops.delete("fail"));
        Assertions.assertEquals("delete failed", ex.getMessage());
        Mockito.verify(blockingOperator).close();
    }
}
