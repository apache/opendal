package org.apache.opendal.layer;

import org.apache.opendal.Layer;

public class CompleteLayer extends Layer{

    @Override
    protected long layer(long nativeOp) {
        return doLayer(nativeOp);
    }
    
    private static native long doLayer(long nativeHandle);
}
