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

package org.apache.opendal.test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.WriteOptions;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class JniMapValidationTest {
    private static Stream<Arguments> invalidMaps() {
        return Stream.of(
                Arguments.of("non-string value", "root", 12345),
                Arguments.of("non-string key", 12345, "/"),
                Arguments.of("null value", "root", null),
                Arguments.of("null key", null, "/"));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Map<String, String> rawMapWith(Object key, Object value) {
        final Map map = new HashMap();
        map.put(key, value);
        return (Map<String, String>) map;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidMaps")
    void testOperatorOfRejectsInvalidMapEntries(String label, Object key, Object value) {
        final Map<String, String> map = rawMapWith(key, value);
        assertThatThrownBy(() -> Operator.of("memory", map))
                .as(label)
                .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.ConfigInvalid));
        assertThatThrownBy(() -> AsyncOperator.of("memory", map))
                .as(label)
                .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.ConfigInvalid));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidMaps")
    void testWriteOptionsUserMetadataRejectsInvalidMapEntries(String label, Object key, Object value) {
        final WriteOptions options =
                WriteOptions.builder().userMetadata(rawMapWith(key, value)).build();
        try (final Operator op = Operator.of(ServiceConfig.Memory.builder().build())) {
            assertThatThrownBy(() -> op.write("test.txt", "hello", options))
                    .as(label)
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.ConfigInvalid));
        }
        try (final AsyncOperator op =
                AsyncOperator.of(ServiceConfig.Memory.builder().build())) {
            assertThatThrownBy(() -> op.write("test.txt", "hello", options))
                    .as(label)
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.ConfigInvalid));
        }
    }
}
