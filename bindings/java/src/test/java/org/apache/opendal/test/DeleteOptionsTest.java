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
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.DeleteOptions;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException.Code;
import org.apache.opendal.Operator;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class DeleteOptionsTest {
    @TempDir
    Path root;

    @ParameterizedTest
    @ValueSource(strings = {"memory", "fs"})
    void testBlockingRecursiveDelete(String scheme) {
        try (AsyncOperator async = newOperator(scheme);
                Operator op = async.blocking()) {
            op.write("tree/nested/file", "content");
            op.write("tree-sibling/file", "keep");

            assertThatThrownBy(() -> op.delete("tree/")).is(OpenDALExceptionCondition.ofSync(Code.Unexpected));
            assertThat(op.stat("tree/nested/file").isFile()).isTrue();

            op.delete("tree/", DeleteOptions.builder().recursive(true).build());

            assertThatThrownBy(() -> op.stat("tree/nested/file")).is(OpenDALExceptionCondition.ofSync(Code.NotFound));
            assertThatThrownBy(() -> op.stat("tree/")).is(OpenDALExceptionCondition.ofSync(Code.NotFound));
            assertThat(op.stat("tree-sibling/file").isFile()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"memory", "fs"})
    void testAsyncRecursiveDelete(String scheme) {
        try (AsyncOperator async = newOperator(scheme)) {
            async.write("tree/nested/file", "content").join();

            assertThatThrownBy(async.delete("tree/")::join).is(OpenDALExceptionCondition.ofAsync(Code.Unexpected));
            assertThat(async.stat("tree/nested/file").join().isFile()).isTrue();

            async.delete("tree/", DeleteOptions.builder().recursive(true).build())
                    .join();

            assertThatThrownBy(async.stat("tree/nested/file")::join)
                    .is(OpenDALExceptionCondition.ofAsync(Code.NotFound));
            assertThatThrownBy(async.stat("tree/")::join).is(OpenDALExceptionCondition.ofAsync(Code.NotFound));
        }
    }

    static Stream<Arguments> unsupportedOptions() {
        return Stream.of(
                Arguments.of("version", DeleteOptions.builder().version("v1").build()),
                Arguments.of(
                        "ifMatch", DeleteOptions.builder().ifMatch("\"etag\"").build()),
                Arguments.of(
                        "ifNoneMatch",
                        DeleteOptions.builder().ifNoneMatch("\"etag\"").build()),
                Arguments.of(
                        "ifVersionMatch",
                        DeleteOptions.builder().ifVersionMatch("v1").build()),
                Arguments.of(
                        "ifVersionNotMatch",
                        DeleteOptions.builder().ifVersionNotMatch("v1").build()),
                Arguments.of("ifNotChanged with an ETag", deleteIfNotChanged("\"etag\"", null)),
                Arguments.of("ifNotChanged with a version", deleteIfNotChanged(null, "v1")));
    }

    private static DeleteOptions deleteIfNotChanged(String etag, String version) {
        Metadata snapshot = new Metadata(0, 7, null, null, null, null, etag, null, version);
        return DeleteOptions.builder().ifNotChanged(snapshot).build();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedOptions")
    void testUnsupportedDeleteOption(String name, DeleteOptions options) {
        try (AsyncOperator async = newOperator("memory");
                Operator op = async.blocking()) {
            op.write("file", "content");

            assertThatThrownBy(() -> op.delete("file", options)).is(OpenDALExceptionCondition.ofSync(Code.Unsupported));
            assertThatThrownBy(async.delete("file", options)::join)
                    .is(OpenDALExceptionCondition.ofAsync(Code.Unsupported));
            assertThat(op.stat("file").isFile()).isTrue();
        }
    }

    @Test
    void testMetadataWithoutIdentityIsRejected() {
        try (AsyncOperator async = newOperator("memory");
                Operator op = async.blocking()) {
            op.write("file", "content");
            DeleteOptions options =
                    DeleteOptions.builder().ifNotChanged(op.stat("file")).build();

            assertThatThrownBy(() -> op.delete("file", options))
                    .is(OpenDALExceptionCondition.ofSync(Code.ConfigInvalid));
            assertThatThrownBy(async.delete("file", options)::join)
                    .is(OpenDALExceptionCondition.ofAsync(Code.ConfigInvalid));
            assertThat(op.stat("file").isFile()).isTrue();
        }
    }

    private AsyncOperator newOperator(String scheme) {
        return AsyncOperator.of(
                scheme,
                "fs".equals(scheme) ? Collections.singletonMap("root", root.toString()) : Collections.emptyMap());
    }
}
