package org.apache.opendal;

import io.questdb.jar.jni.JarJniLoader;

public abstract class OpenDALObject implements AutoCloseable {
    private static final String ORG_APACHE_OPENDAL_RUST_LIBS = "/org/apache/opendal/rust/libs";

    private static final String OPENDAL_JAVA = "opendal_java";

    static {
        JarJniLoader.loadLib(
            Operator.class,
            ORG_APACHE_OPENDAL_RUST_LIBS,
            OPENDAL_JAVA);
    }

    long ptr;
}
