package org.apache.opendal.test;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.Cleanup;
import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.Operator;
import org.junit.jupiter.api.Test;

public class AsyncExecutorTest {
    @Test
    void testOperatorWithRetryLayer() {
        final Map<String, String> conf = new HashMap<>();
        conf.put("root", "/opendal/");
        final int cores = Runtime.getRuntime().availableProcessors();
        @Cleanup final AsyncExecutor executor = AsyncExecutor.createTokioExecutor(cores);
        @Cleanup final Operator op = Operator.of("memory", conf, executor);
        assertThat(op.info).isNotNull();

        final String key = "key";
        final byte[] v0 = "v0".getBytes(StandardCharsets.UTF_8);
        final byte[] v1 = "v1".getBytes(StandardCharsets.UTF_8);
        op.write(key, v0).join();
        assertThat(op.read(key).join()).isEqualTo(v0);
        op.write(key, v1).join();
        assertThat(op.read(key).join()).isEqualTo(v1);
    }
}
