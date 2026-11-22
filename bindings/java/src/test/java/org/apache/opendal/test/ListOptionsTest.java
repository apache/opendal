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
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Entry;
import org.apache.opendal.ListOptions;
import org.apache.opendal.OpenDALException.Code;
import org.apache.opendal.Operator;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ListOptionsTest {
    @TempDir
    Path root;

    @ParameterizedTest
    @ValueSource(strings = {"memory", "fs"})
    void testListWithGlob(String scheme) {
        try (AsyncOperator async = newOperator(scheme);
                Operator op = async.blocking()) {
            String dir = "glob-dir/";
            for (String name : Arrays.asList("a.jpg", "b.png", "nested/c.jpg", "nested/猫.jpg")) {
                op.write(dir + name, "data");
            }

            ListOptions recursiveGlob =
                    ListOptions.builder().glob("**/?.jpg").limit(1).build();
            assertThat(relativePaths(op.list(dir, recursiveGlob), dir))
                    .containsExactly("a.jpg", "nested/c.jpg", "nested/猫.jpg");
            assertThat(relativePaths(async.list(dir, recursiveGlob).join(), dir))
                    .containsExactly("a.jpg", "nested/c.jpg", "nested/猫.jpg");

            ListOptions shallowGlob =
                    ListOptions.builder().recursive(true).glob("?.jpg").build();
            assertThat(relativePaths(op.list(dir, shallowGlob), dir)).containsExactly("a.jpg");
            assertThat(relativePaths(async.list(dir, shallowGlob).join(), dir)).containsExactly("a.jpg");
        }
    }

    @Test
    void testListWithGlobAndStartAfter() {
        try (AsyncOperator async = newOperator("memory");
                Operator op = async.blocking()) {
            String dir = "glob-cursor/";
            for (String name : Arrays.asList("a.jpg", "b.png", "nested/c.jpg", "nested/猫.jpg")) {
                op.write(dir + name, "data");
            }

            ListOptions options = ListOptions.builder()
                    .glob("**/?.jpg")
                    .startAfter(dir + "nested/c.jpg")
                    .build();
            assertThat(relativePaths(op.list(dir, options), dir)).containsExactly("nested/猫.jpg");
            assertThat(relativePaths(async.list(dir, options).join(), dir)).containsExactly("nested/猫.jpg");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "["})
    void testInvalidGlobPatternIsRejected(String pattern) {
        try (AsyncOperator async = newOperator("memory");
                Operator op = async.blocking()) {
            op.write("dir/a.txt", "data");
            ListOptions options = ListOptions.builder().glob(pattern).build();

            assertThatThrownBy(() -> op.list("dir/", options)).is(OpenDALExceptionCondition.ofSync(Code.ConfigInvalid));
            assertThatThrownBy(async.list("dir/", options)::join)
                    .is(OpenDALExceptionCondition.ofAsync(Code.ConfigInvalid));
        }
    }

    static Stream<Arguments> unsupportedOptions() {
        return Stream.of(
                Arguments.of(
                        "startAfter",
                        ListOptions.builder().startAfter("dir/file").build()),
                Arguments.of(
                        "startAfter with recursive",
                        ListOptions.builder()
                                .recursive(true)
                                .startAfter("dir/file")
                                .build()),
                Arguments.of("versions", ListOptions.builder().versions(true).build()),
                Arguments.of("deleted", ListOptions.builder().deleted(true).build()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedOptions")
    void testUnsupportedListOptionsAreRejected(String name, ListOptions options) {
        try (AsyncOperator async = AsyncOperator.of("dashmap", Collections.emptyMap());
                Operator op = async.blocking()) {
            op.write("dir/file", "data");

            assertThatThrownBy(() -> op.list("dir/", options)).is(OpenDALExceptionCondition.ofSync(Code.Unsupported));
            assertThatThrownBy(async.list("dir/", options)::join)
                    .is(OpenDALExceptionCondition.ofAsync(Code.Unsupported));
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -2})
    void testInvalidLimitDoesNotLeakAsyncFutures(long invalidLimit) throws Exception {
        Class<?> registryType = Class.forName(AsyncOperator.class.getName() + "$AsyncRegistry");
        Field registryField = registryType.getDeclaredField("registry");
        registryField.setAccessible(true);
        Map<?, ?> registry = (Map<?, ?>) registryField.get(registryType.getEnumConstants()[0]);
        Set<?> pendingBefore = new HashSet<>(registry.keySet());

        ListOptions invalid = ListOptions.builder().limit(invalidLimit).build();
        try (AsyncOperator async = newOperator("memory");
                Operator op = async.blocking()) {
            assertThatThrownBy(() -> op.list("dir/", invalid)).is(OpenDALExceptionCondition.ofSync(Code.Unexpected));
            assertThatThrownBy(() -> async.list("dir/", invalid)).is(OpenDALExceptionCondition.ofSync(Code.Unexpected));
        }

        assertThat(pendingBefore.containsAll(registry.keySet()))
                .as("invalid list options must not leave registered futures")
                .isTrue();
    }

    private static List<String> relativePaths(List<Entry> entries, String prefix) {
        return entries.stream()
                .map(Entry::getPath)
                .map(path -> path.substring(prefix.length()))
                .sorted()
                .collect(Collectors.toList());
    }

    private AsyncOperator newOperator(String scheme) {
        return AsyncOperator.of(
                scheme,
                "fs".equals(scheme) ? Collections.singletonMap("root", root.toString()) : Collections.emptyMap());
    }
}
