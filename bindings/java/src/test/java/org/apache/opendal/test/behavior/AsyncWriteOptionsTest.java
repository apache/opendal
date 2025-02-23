package org.apache.opendal.test.behavior;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.util.UUID;
import org.apache.opendal.OpenDALException.Code;
import org.apache.opendal.WriteOptions;
import org.apache.opendal.test.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;

public class AsyncWriteOptionsTest extends BehaviorTestBase {

    @Test
    void testIfNotExists() {
        assumeTrue(asyncOp().info.fullCapability.writeWithIfNotExists);
        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        WriteOptions options = WriteOptions.builder().ifNotExists(true).build();
        asyncOp().write(path, content, options).join();

        assertThatThrownBy(() -> asyncOp().write(path, content, options).join())
                .is(OpenDALExceptionCondition.ofAsync(Code.ConditionNotMatch));
    }

    @Test
    void testWriteWithCacheControl() {
        assumeTrue(asyncOp().info.fullCapability.writeWithCacheControl);
        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();
        final String cacheControl = "max-age=3600";

        WriteOptions options = WriteOptions.builder().cacheControl(cacheControl).build();

        asyncOp().write(path, content, options).join();

        String actualCacheControl = asyncOp().stat(path).join().getCacheControl();
        assertThat(actualCacheControl).isEqualTo(cacheControl);
    }

    @Test
    void testWriteWithIfNoneMatch() {
        assumeTrue(asyncOp().info.fullCapability.writeWithIfNoneMatch);
        final String path = UUID.randomUUID().toString();
        final byte[] content = generateBytes();

        asyncOp().write(path, content).join();
        String etag = asyncOp().stat(path).join().getEtag();

        WriteOptions options = WriteOptions.builder().ifNoneMatch(etag).build();

        assertThatThrownBy(() -> asyncOp().write(path, content, options).join())
                .is(OpenDALExceptionCondition.ofAsync(Code.ConditionNotMatch));
    }

    @Test
    void testWriteWithIfMatch() {
        assumeTrue(asyncOp().info.fullCapability.writeWithIfMatch);

        final String pathA = UUID.randomUUID().toString();
        final String pathB = UUID.randomUUID().toString();
        final byte[] contentA = generateBytes();
        final byte[] contentB = generateBytes();

        asyncOp().write(pathA, contentA).join();
        asyncOp().write(pathB, contentB).join();

        String etagA = asyncOp().stat(pathA).join().getEtag();
        String etagB = asyncOp().stat(pathB).join().getEtag();

        WriteOptions optionsA = WriteOptions.builder().ifMatch(etagA).build();

        asyncOp().write(pathA, contentA, optionsA).join();

        WriteOptions optionsB = WriteOptions.builder().ifMatch(etagB).build();

        assertThatThrownBy(() -> asyncOp().write(pathA, contentA, optionsB).join())
                .is(OpenDALExceptionCondition.ofAsync(Code.ConditionNotMatch));
    }

    @Test
    void testWriteWithAppend() {
        assumeTrue(asyncOp().info.fullCapability.writeCanAppend);

        final String path = UUID.randomUUID().toString();
        final byte[] contentOne = "Test".getBytes();
        final byte[] contentTwo = " Data".getBytes();
        asyncOp().write(path, contentOne).join();

        WriteOptions appendOptions = WriteOptions.builder().append(true).build();
        asyncOp().write(path, contentTwo, appendOptions);

        byte[] result = asyncOp().read(path).join();
        assertThat(result.length).isEqualTo(contentOne.length + contentTwo.length);
        assertThat(result).isEqualTo("Test Data".getBytes());
    }
}
