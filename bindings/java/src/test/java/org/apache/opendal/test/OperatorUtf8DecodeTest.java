package org.apache.opendal.test;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.opendal.BlockingOperator;
import org.apache.opendal.Metadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class OperatorUtf8DecodeTest {
    @TempDir
    private static Path tempDir;

    /**
     * Write file with non ascii name should succeed.
     *
     * @see <a href="https://github.com/apache/incubator-opendal/issues/3194">More information</a>
     */
    @Test
    public void testWriteFileWithNonAsciiName() {
        final Map<String, String> conf = new HashMap<>();
        conf.put("root", tempDir.toString());

        try (final BlockingOperator op = BlockingOperator.of("fs", conf)) {
            final String path = "❌😱中文.test";
            final byte[] content = "❌😱中文".getBytes();
            op.write(path, content);
            final Metadata meta = op.stat(path);
            assertThat(meta.isFile()).isTrue();
            assertThat(meta.getContentLength()).isEqualTo(content.length);

            op.delete(path);
        }
    }
}
