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
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Entry;
import org.apache.opendal.ListOptions;
import org.apache.opendal.OpenDALException.Code;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ListOptionsTest {
    @Test
    void testGlobFiltersBlockingAndAsyncLists() {
        try (AsyncOperator async =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                Operator op = async.blocking()) {
            assertThat(op.info.capability.listWithGlob).isTrue();
            op.write("dir/one.txt", "one");
            op.write("dir/nested/two.txt", "two");
            op.write("dir/three.bin", "three");
            op.write("outside.txt", "outside");
            ListOptions options = ListOptions.builder().glob("**/*.txt").build();

            assertThat(op.list("dir/", options))
                    .extracting(Entry::getPath)
                    .containsExactlyInAnyOrder("dir/one.txt", "dir/nested/two.txt");
            assertThat(async.list("dir/", options).join())
                    .extracting(Entry::getPath)
                    .containsExactlyInAnyOrder("dir/one.txt", "dir/nested/two.txt");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "["})
    void testInvalidGlob(String glob) {
        try (AsyncOperator async =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                Operator op = async.blocking()) {
            ListOptions options = ListOptions.builder().glob(glob).build();
            assertThatThrownBy(() -> op.list("/", options)).is(OpenDALExceptionCondition.ofSync(Code.ConfigInvalid));
            assertThatThrownBy(async.list("/", options)::join)
                    .is(OpenDALExceptionCondition.ofAsync(Code.ConfigInvalid));
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -2})
    void testInvalidLimitKeepsUnexpectedCode(long limit) {
        try (AsyncOperator async =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                Operator op = async.blocking()) {
            ListOptions options = ListOptions.builder().limit(limit).build();
            assertThatThrownBy(() -> op.list("/", options)).is(OpenDALExceptionCondition.ofSync(Code.Unexpected));
            assertThatThrownBy(() -> async.list("/", options)).is(OpenDALExceptionCondition.ofSync(Code.Unexpected));
        }
    }

    @Test
    void testToBuilder() {
        ListOptions original =
                ListOptions.builder().glob("*.txt").recursive(true).build();
        ListOptions updated = original.toBuilder().glob("*.bin").build();
        assertThat(updated.glob).isEqualTo("*.bin");
        assertThat(updated.recursive).isTrue();
        assertThat(updated.limit).isEqualTo(-1L);
        assertThat(original.glob).isEqualTo("*.txt");
        assertThat(original).isNotEqualTo(original.toBuilder().build());
    }
}
