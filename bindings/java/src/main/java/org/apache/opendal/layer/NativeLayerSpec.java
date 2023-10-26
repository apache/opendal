package org.apache.opendal.layer;

public abstract class NativeLayerSpec {

    /**
     * This method is called from native code. It returns the pointer of the constructed native layer,
     * which is immediately used (moved) to make the operator.
     */
    @SuppressWarnings("unused")
    protected abstract long makeNativeLayer();
}
