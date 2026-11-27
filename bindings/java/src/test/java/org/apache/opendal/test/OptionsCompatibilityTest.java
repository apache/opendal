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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Instant;
import org.apache.opendal.ListOptions;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.ReadOptions;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.StatOptions;
import org.apache.opendal.WriteOptions;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OptionsCompatibilityTest {
    @ParameterizedTest
    @ValueSource(ints = {0, -2})
    void listAndWriteKeepUnexpectedForInvalidSizes(int invalid) {
        try (Operator op = Operator.of(ServiceConfig.Memory.builder().build())) {
            ListOptions list = ListOptions.builder().limit(invalid).build();
            WriteOptions chunk = WriteOptions.builder().chunk(invalid).build();
            WriteOptions concurrent = WriteOptions.builder().concurrent(invalid).build();
            assertThatThrownBy(() -> op.list("", list))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.Unexpected));
            assertThatThrownBy(() -> op.write("file", "data", chunk))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.Unexpected));
            assertThatThrownBy(() -> op.write("file", "data", concurrent))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.Unexpected));
        }
    }

    @Test
    void readValidatesExecutionOptionsBeforeRange() {
        try (Operator op = Operator.of(ServiceConfig.Memory.builder().build())) {
            ReadOptions options = ReadOptions.builder().offset(-1).concurrent(0).build();
            assertThatThrownBy(() -> op.read("file", options))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.ConfigInvalid));
        }
    }

    @Test
    void statUsesSetterValuesInNativeCalls() {
        try (Operator op = Operator.of(ServiceConfig.Memory.builder().build())) {
            op.write("file", "data");
            StatOptions options = StatOptions.builder().build();
            options.setIfModifiedSince(Instant.MAX);
            assertThat(options.getIfModifiedSince()).isEqualTo(Instant.MAX);
            assertThatThrownBy(() -> op.stat("file", options))
                    .is(OpenDALExceptionCondition.ofSync(OpenDALException.Code.Unexpected));
        }
    }
}
