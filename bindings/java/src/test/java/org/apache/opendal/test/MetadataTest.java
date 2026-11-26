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

import static org.apache.opendal.test.behavior.BehaviorTestBase.generateBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Metadata;
import org.apache.opendal.Metadata.EntryMode;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.WriteOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MetadataTest {
    private static final Instant MODIFIED = Instant.parse("2024-07-15T01:02:03.456789Z");

    @TempDir
    private static Path tempDir;

    @Test
    public void testAsyncMetadata() {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();

        try (final AsyncOperator op = AsyncOperator.of(fs)) {
            final String dir = UUID.randomUUID() + "/";
            op.createDir(dir).join();
            final Metadata dirMetadata = op.stat(dir).join();
            assertTrue(dirMetadata.isDir());

            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            op.write(path, content).join();

            final Metadata metadata = op.stat(path).join();
            assertTrue(metadata.isFile());
            assertThat(metadata.contentLength).isEqualTo(content.length);
            assertThat(metadata.lastModified).isNotNull();
            assertThat(metadata.cacheControl).isNull();
            assertThat(metadata.contentDisposition).isNull();
            assertThat(metadata.contentMd5).isNull();
            assertThat(metadata.contentType).isNull();
            assertThat(metadata.etag).isNull();
            assertThat(metadata.version).isNull();
            assertThat(metadata.userMetadata).isEmpty();

            op.delete(dir).join();
            op.delete(path).join();
        }
    }

    @Test
    public void testBlockingMetadata() {
        final ServiceConfig.Fs fs =
                ServiceConfig.Fs.builder().root(tempDir.toString()).build();

        try (final Operator op = Operator.of(fs)) {
            final String dir = UUID.randomUUID() + "/";
            op.createDir(dir);
            final Metadata dirMetadata = op.stat(dir);
            assertTrue(dirMetadata.isDir());

            final String path = UUID.randomUUID().toString();
            final byte[] content = generateBytes();
            op.write(path, content);

            final Metadata metadata = op.stat(path);
            assertTrue(metadata.isFile());
            assertThat(metadata.contentLength).isEqualTo(content.length);
            assertThat(metadata.lastModified).isNotNull();
            assertThat(metadata.cacheControl).isNull();
            assertThat(metadata.contentDisposition).isNull();
            assertThat(metadata.contentMd5).isNull();
            assertThat(metadata.contentType).isNull();
            assertThat(metadata.etag).isNull();
            assertThat(metadata.version).isNull();
            assertThat(metadata.userMetadata).isEmpty();

            op.delete(dir);
            op.delete(path);
        }
    }

    @Test
    public void testExistingConstructors() {
        final Metadata file = new Metadata(
                0,
                5_000_000_000L,
                "attachment",
                "example-md5",
                "text/plain",
                "max-age=60",
                "example-etag",
                MODIFIED,
                "example-version");
        assertConstructorValues(file, EntryMode.FILE);
        assertThat(file.userMetadata).isEmpty();
        assertThat(file.getUserMetadata()).isEmpty();

        final Map<String, String> custom = Collections.singletonMap("owner", "opendal");
        final Metadata dir = new Metadata(
                1,
                5_000_000_000L,
                "attachment",
                "example-md5",
                "text/plain",
                "max-age=60",
                "example-etag",
                MODIFIED,
                "example-version",
                custom);
        assertConstructorValues(dir, EntryMode.DIR);
        assertThat(dir.userMetadata).isSameAs(custom);
        assertThat(dir.getUserMetadata()).isSameAs(custom);

        final Metadata unknown = new Metadata(2, 0L, null, null, null, null, null, null, null, null);
        assertThat(unknown.mode).isEqualTo(EntryMode.UNKNOWN);
        assertThat(unknown.isFile()).isFalse();
        assertThat(unknown.isDir()).isFalse();
        assertThat(unknown.userMetadata).isNull();
        assertThat(unknown.getUserMetadata()).isNull();
    }

    private static void assertConstructorValues(Metadata metadata, EntryMode mode) {
        assertThat(metadata.mode).isEqualTo(mode);
        assertThat(metadata.getMode()).isEqualTo(metadata.mode);
        assertThat(metadata.contentLength).isEqualTo(5_000_000_000L);
        assertThat(metadata.getContentLength()).isEqualTo(metadata.contentLength);
        assertThat(metadata.contentDisposition).isEqualTo("attachment");
        assertThat(metadata.getContentDisposition()).isEqualTo(metadata.contentDisposition);
        assertThat(metadata.contentMd5).isEqualTo("example-md5");
        assertThat(metadata.getContentMd5()).isEqualTo(metadata.contentMd5);
        assertThat(metadata.contentType).isEqualTo("text/plain");
        assertThat(metadata.getContentType()).isEqualTo(metadata.contentType);
        assertThat(metadata.cacheControl).isEqualTo("max-age=60");
        assertThat(metadata.getCacheControl()).isEqualTo(metadata.cacheControl);
        assertThat(metadata.etag).isEqualTo("example-etag");
        assertThat(metadata.getEtag()).isEqualTo(metadata.etag);
        assertThat(metadata.lastModified).isEqualTo(MODIFIED);
        assertThat(metadata.getLastModified()).isEqualTo(metadata.lastModified);
        assertThat(metadata.version).isEqualTo("example-version");
        assertThat(metadata.getVersion()).isEqualTo(metadata.version);
        assertThat(metadata.isFile()).isEqualTo(mode == EntryMode.FILE);
        assertThat(metadata.isDir()).isEqualTo(mode == EntryMode.DIR);
    }

    @Test
    public void testEntryMode() {
        assertThat(EntryMode.values()).containsExactly(EntryMode.FILE, EntryMode.DIR, EntryMode.UNKNOWN);
        assertThat(EntryMode.of(0)).isEqualTo(EntryMode.FILE);
        assertThat(EntryMode.of(1)).isEqualTo(EntryMode.DIR);
        assertThat(EntryMode.of(2)).isEqualTo(EntryMode.UNKNOWN);
        assertThatThrownBy(() -> EntryMode.of(3)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPublicMetadataShape() throws ReflectiveOperationException {
        final Class<?>[] withoutUserMetadata = {
            int.class,
            long.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            Instant.class,
            String.class
        };
        final Class<?>[] withUserMetadata = Arrays.copyOf(withoutUserMetadata, 10);
        withUserMetadata[9] = Map.class;
        assertThat(Metadata.class.getConstructors())
                .containsExactlyInAnyOrder(
                        Metadata.class.getConstructor(withoutUserMetadata),
                        Metadata.class.getConstructor(withUserMetadata));

        final Field[] fields = Metadata.class.getDeclaredFields();
        assertThat(fields)
                .extracting(Field::getName, Field::getType)
                .containsExactlyInAnyOrder(
                        tuple("mode", EntryMode.class),
                        tuple("contentLength", long.class),
                        tuple("contentDisposition", String.class),
                        tuple("contentMd5", String.class),
                        tuple("contentType", String.class),
                        tuple("cacheControl", String.class),
                        tuple("etag", String.class),
                        tuple("lastModified", Instant.class),
                        tuple("version", String.class),
                        tuple("userMetadata", Map.class));
        assertThat(fields)
                .allSatisfy(field -> assertThat(field.getModifiers()).isEqualTo(Modifier.PUBLIC | Modifier.FINAL));
        assertThat(Metadata.class.getField("userMetadata").getGenericType().getTypeName())
                .isEqualTo("java.util.Map<java.lang.String, java.lang.String>");
    }

    @Test
    public void testMemoryMetadataRoundTrip() {
        final byte[] content = {1, 2, 3};
        final WriteOptions options = WriteOptions.builder()
                .contentDisposition("attachment; filename=report.txt")
                .contentType("application/octet-stream")
                .cacheControl("no-cache")
                .userMetadata(Collections.singletonMap("owner", "opendal"))
                .build();

        try (final Operator op = Operator.of(ServiceConfig.Memory.builder().build())) {
            op.write("report.txt", content, options);
            final Metadata metadata = op.stat("report.txt");
            assertThat(metadata.mode).isEqualTo(EntryMode.FILE);
            assertThat(metadata.contentLength).isEqualTo(content.length);
            assertThat(metadata.contentDisposition).isEqualTo(options.contentDisposition);
            assertThat(metadata.contentType).isEqualTo(options.contentType);
            assertThat(metadata.cacheControl).isEqualTo(options.cacheControl);
            assertThat(metadata.userMetadata).isEqualTo(options.userMetadata);
            assertThat(metadata.contentMd5).isNull();
            assertThat(metadata.etag).isNull();
            assertThat(metadata.lastModified).isNull();
            assertThat(metadata.version).isNull();
        }
    }
}
