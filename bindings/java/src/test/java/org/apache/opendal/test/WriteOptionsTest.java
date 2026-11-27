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
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException.Code;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorOutputStream;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.WriteOptions;
import org.apache.opendal.layer.CapabilityCheckLayer;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(10)
public class WriteOptionsTest {
    private static final String CONTENT_LANGUAGE = "fr-CA, en";
    private static final byte[] CONTENT = "content".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path root;

    @Test
    void testContentLanguageRoundTripOnMemory() throws Exception {
        try (AsyncOperator async =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                Operator op = async.blocking()) {
            assertThat(op.info.capability.writeWithContentLanguage).isTrue();
            WriteOptions options =
                    WriteOptions.builder().contentLanguage(CONTENT_LANGUAGE).build();

            op.write("blocking", CONTENT, options);
            assertThat(op.stat("blocking").contentLanguage).isEqualTo(CONTENT_LANGUAGE);
            async.write("async", CONTENT, options).join();
            assertThat(async.stat("async").join().contentLanguage).isEqualTo(CONTENT_LANGUAGE);
            try (OperatorOutputStream out = op.createOutputStream("stream", options)) {
                out.write(CONTENT);
            }
            assertThat(op.stat("stream").contentLanguage).isEqualTo(CONTENT_LANGUAGE);
        }
    }

    @Test
    void testContentLanguageIsUnsupportedOnFilesystem() {
        try (AsyncOperator fs = AsyncOperator.of(
                        ServiceConfig.Fs.builder().root(root.toString()).build());
                AsyncOperator checked = fs.layer(new CapabilityCheckLayer());
                Operator op = checked.blocking()) {
            assertThat(fs.info.capability.writeWithContentLanguage).isFalse();
            WriteOptions options =
                    WriteOptions.builder().contentLanguage(CONTENT_LANGUAGE).build();
            assertThatThrownBy(() -> op.write("file", CONTENT, options))
                    .is(OpenDALExceptionCondition.ofSync(Code.Unsupported));
        }
    }

    static Stream<Arguments> invalidOptions() {
        return Stream.of(
                Arguments.of(WriteOptions.builder().concurrent(0).build(), Code.Unexpected),
                Arguments.of(WriteOptions.builder().concurrent(-2).build(), Code.Unexpected),
                Arguments.of(WriteOptions.builder().chunk(0).build(), Code.Unexpected),
                Arguments.of(WriteOptions.builder().chunk(-2).build(), Code.Unexpected),
                Arguments.of(WriteOptions.builder().expires(Duration.ZERO).build(), Code.ConfigInvalid),
                Arguments.of(
                        WriteOptions.builder().expires(Duration.ofSeconds(-1)).build(), Code.ConfigInvalid),
                Arguments.of(
                        WriteOptions.builder().expires(Duration.ofNanos(-1)).build(), Code.ConfigInvalid));
    }

    @ParameterizedTest
    @MethodSource("invalidOptions")
    void testInvalidOptions(WriteOptions options, Code code) {
        try (AsyncOperator async =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                Operator op = async.blocking()) {
            assertThatThrownBy(() -> op.write("file", CONTENT, options)).is(OpenDALExceptionCondition.ofSync(code));
            assertThatThrownBy(() -> async.write("file", CONTENT, options)).is(OpenDALExceptionCondition.ofSync(code));
        }
    }

    @Test
    void testIfNotChangedRequiresMetadataIdentity() {
        try (AsyncOperator async =
                        AsyncOperator.of(ServiceConfig.Memory.builder().build());
                Operator op = async.blocking()) {
            WriteOptions options = WriteOptions.builder()
                    .ifNotChanged(Metadata.builder().build())
                    .build();
            assertThatThrownBy(() -> op.write("file", CONTENT, options))
                    .is(OpenDALExceptionCondition.ofSync(Code.ConfigInvalid));
            assertThatThrownBy(async.write("file", CONTENT, options)::join)
                    .is(OpenDALExceptionCondition.ofAsync(Code.ConfigInvalid));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"memory", "fs"})
    void testUnsupportedConditionsAndExpires(String scheme) {
        ServiceConfig config = "memory".equals(scheme)
                ? ServiceConfig.Memory.builder().build()
                : ServiceConfig.Fs.builder().root(root.toString()).build();
        try (AsyncOperator async = AsyncOperator.of(config);
                Operator op = async.blocking()) {
            assertThat(op.info.capability.writeWithIfVersionMatch).isFalse();
            assertThat(op.info.capability.writeWithIfVersionNotMatch).isFalse();
            assertThat(op.info.capability.writeWithIfMatch).isFalse();
            assertThat(op.info.capability.writeWithExpires).isFalse();

            op.write("file", CONTENT);
            Metadata snapshot = Metadata.builder()
                    .etag("\"etag\"")
                    .lastModified(Instant.MAX)
                    .build();
            for (WriteOptions options : new WriteOptions[] {
                WriteOptions.builder().ifVersionMatch("17").build(),
                WriteOptions.builder().ifVersionNotMatch("18").build(),
                WriteOptions.builder().ifNotChanged(snapshot).build(),
                WriteOptions.builder().expires(Duration.ofNanos(1)).build(),
                WriteOptions.builder()
                        .expires(Duration.ofSeconds(Long.MAX_VALUE, 999_999_999))
                        .build()
            }) {
                assertThatThrownBy(() -> op.write("file", "updated", options))
                        .is(OpenDALExceptionCondition.ofSync(Code.Unsupported));
                assertThatThrownBy(async.write("file", "updated", options)::join)
                        .is(OpenDALExceptionCondition.ofAsync(Code.Unsupported));
            }
            assertThat(op.read("file")).isEqualTo(CONTENT);
        }
    }

    @Test
    void testVersionConditionsReachGcsUploadRequest() throws Exception {
        List<String> queries = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
        server.createContext("/upload/storage/v1/b/bucket/o", exchange -> {
            try {
                exchange.getRequestBody().close();
                queries.add(exchange.getRequestURI().getQuery());
                byte[] metadata = "{\"size\":\"7\",\"generation\":\"20\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, metadata.length);
                exchange.getResponseBody().write(metadata);
            } finally {
                exchange.close();
            }
        });
        server.start();
        try (AsyncOperator async = AsyncOperator.of(ServiceConfig.Gcs.builder()
                        .bucket("bucket")
                        .endpoint(endpoint)
                        .skipSignature(true)
                        .build());
                Operator op = async.blocking()) {
            assertThat(op.info.capability.writeWithIfVersionMatch).isTrue();
            assertThat(op.info.capability.writeWithIfVersionNotMatch).isTrue();
            WriteOptions direct = WriteOptions.builder()
                    .ifVersionMatch("17")
                    .ifVersionNotMatch("18")
                    .build();
            op.write("file", CONTENT, direct);

            Metadata snapshot = Metadata.builder()
                    .version("19")
                    .etag("\"etag\"")
                    .lastModified(Instant.MAX)
                    .build();
            WriteOptions fromSnapshot = direct.toBuilder()
                    .ifVersionMatch(null)
                    .ifNotChanged(snapshot)
                    .build();
            async.write("file", CONTENT, fromSnapshot).join();
        } finally {
            server.stop(0);
        }

        assertThat(queries).hasSize(2);
        assertThat(queries.get(0)).contains("uploadType=media", "ifGenerationMatch=17", "ifGenerationNotMatch=18");
        assertThat(queries.get(1)).contains("uploadType=media", "ifGenerationMatch=19", "ifGenerationNotMatch=18");
    }

    @Test
    void testToBuilder() {
        WriteOptions original = WriteOptions.builder()
                .contentLanguage(CONTENT_LANGUAGE)
                .expires(Duration.ofSeconds(60))
                .build();
        WriteOptions updated = original.toBuilder().chunk(1024).build();
        assertThat(updated.contentLanguage).isEqualTo(CONTENT_LANGUAGE);
        assertThat(updated.expires).isEqualTo(original.expires);
        assertThat(updated.chunk).isEqualTo(1024);
        assertThat(original.chunk).isEqualTo(-1);
        assertThat(original).isNotEqualTo(original.toBuilder().build());
    }
}
