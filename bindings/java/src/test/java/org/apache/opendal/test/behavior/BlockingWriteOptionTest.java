package org.apache.opendal.test.behavior;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.util.UUID;
import org.apache.opendal.WriteOptions;
import org.junit.jupiter.api.Test;

public class BlockingWriteOptionTest extends BehaviorTestBase {

    @Test
    void testWriteWithCacheControl() {
        assumeTrue(op().info.fullCapability.writeWithCacheControl);

        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        final String cacheControl = "max-age=3600";

        WriteOptions options = WriteOptions.builder().cacheControl(cacheControl).build();
        op().write(path, content, options);

        String actualCacheControl = op().stat(path).getCacheControl();
        assertThat(actualCacheControl).isEqualTo(cacheControl);
    }

    @Test
    void testWriteWithContentType() {
        assumeTrue(op().info.fullCapability.writeWithContentType);

        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        final String contentType = "application/json";

        WriteOptions options = WriteOptions.builder().contentType(contentType).build();
        op().write(path, content, options);

        String actualContentType = op().stat(path).getContentType();
        assertThat(actualContentType).isEqualTo(contentType);
    }

    @Test
    void testWriteWithContentDisposition() {
        assumeTrue(op().info.fullCapability.writeWithContentDisposition);

        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        final String disposition = "attachment; filename=\"test.txt\"";

        WriteOptions options =
                WriteOptions.builder().contentDisposition(disposition).build();
        op().write(path, content, options);

        String actualDisposition = op().stat(path).getContentDisposition();
        assertThat(actualDisposition).isEqualTo(disposition);
    }

    @Test
    void testWriteWithAppend() {
        assumeTrue(op().info.fullCapability.writeCanAppend);

        final String path = UUID.randomUUID().toString();
        final byte[] contentOne = "Test".getBytes();
        final byte[] contentTwo = " Data".getBytes();

        op().write(path, contentOne);
        WriteOptions appendOptions = WriteOptions.builder().append(true).build();
        op().write(path, contentTwo, appendOptions);

        byte[] result = op().read(path);
        assertThat(result.length).isEqualTo(contentOne.length + contentTwo.length);
        assertThat(result).isEqualTo("Test Data".getBytes());
    }
}
