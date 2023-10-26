package org.apache.opendal.layer;

import java.time.Duration;
import lombok.Builder;

@Builder
public class RetryNativeLayerSpec extends NativeLayerSpec {

    private final boolean jitter;

    @Builder.Default
    private final float factor = 2;

    @Builder.Default
    private final Duration minDelay = Duration.ofSeconds(1);

    @Builder.Default
    private final Duration maxDelay = Duration.ofSeconds(60);

    @Builder.Default
    private final long maxTimes = 3;

    @Override
    protected long makeNativeLayer() {
        return makeNativeLayer(jitter, factor, minDelay.toNanos(), maxDelay.toNanos(), maxTimes);
    }

    private static native long makeNativeLayer(boolean jitter, float factor, long minDelay, long maxDelay, long maxTimes);
}
