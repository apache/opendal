package org.apache.opendal;

public class AsyncExecutor extends NativeObject {
    public static AsyncExecutor createTokioExecutor(int cores) {
        return new AsyncExecutor(makeTokioExecutor(cores));
    }

    private AsyncExecutor(long nativeHandle) {
        super(nativeHandle);
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long makeTokioExecutor(int cores);
}
