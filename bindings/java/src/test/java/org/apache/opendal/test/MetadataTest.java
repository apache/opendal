package org.apache.opendal.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.opendal.BlockingOperator;
import org.apache.opendal.Metadata;
import org.apache.opendal.Operator;
import org.apache.opendal.test.behavior.AbstractBehaviorTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MetadataTest {
    @TempDir
    private static Path tempDir;

    @Test
    public void testAsyncMetadata() {
        final Map<String, String> conf = new HashMap<>();
        conf.put("root", tempDir.toString());

        try (final Operator op = Operator.of("fs", conf)) {
            final String dir = String.format("%s/", UUID.randomUUID().toString());
            op.createDir(dir).join();
            final Metadata dirMetadata = op.stat(dir).join();
            assertTrue(dirMetadata.isDir());

            final String path = UUID.randomUUID().toString();
            final byte[] content = AbstractBehaviorTest.generateBytes();
            op.write(path, content).join();

            final Metadata metadata = op.stat(path).join();
            assertTrue(metadata.isFile());
            assertThat(metadata.contentLength).isEqualTo(content.length);
            assertThat(metadata.lastModified).isNotNull();
            assertThat(metadata.cacheControl).isNull();
            assertThat(metadata.contentDisposition).isNull();
            assertThat(metadata.contentMd5).isNull();
            assertThat(metadata.contentRange).isNull();
            assertThat(metadata.contentType).isNull();
            assertThat(metadata.etag).isNull();
            assertThat(metadata.version).isNull();

            op.delete(dir).join();
            op.delete(path).join();
        }
    }

    @Test
    public void testBlockingMetadata() {
        final Map<String, String> conf = new HashMap<>();
        conf.put("root", tempDir.toString());

        try (final BlockingOperator op = BlockingOperator.of("fs", conf)) {
            final String dir = String.format("%s/", UUID.randomUUID().toString());
            op.createDir(dir);
            final Metadata dirMetadata = op.stat(dir);
            assertTrue(dirMetadata.isDir());

            final String path = UUID.randomUUID().toString();
            final byte[] content = AbstractBehaviorTest.generateBytes();
            op.write(path, content);

            final Metadata metadata = op.stat(path);
            assertTrue(metadata.isFile());
            assertThat(metadata.contentLength).isEqualTo(content.length);
            assertThat(metadata.lastModified).isNotNull();
            assertThat(metadata.cacheControl).isNull();
            assertThat(metadata.contentDisposition).isNull();
            assertThat(metadata.contentMd5).isNull();
            assertThat(metadata.contentRange).isNull();
            assertThat(metadata.contentType).isNull();
            assertThat(metadata.etag).isNull();
            assertThat(metadata.version).isNull();

            op.delete(dir);
            op.delete(path);
        }
    }
}
