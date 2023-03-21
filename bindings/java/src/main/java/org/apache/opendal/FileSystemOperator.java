package org.apache.opendal;

import io.questdb.jar.jni.JarJniLoader;


public class FileSystemOperator implements Operator {

    String root;
    long ptr;


    public static final String ORG_APACHE_OPENDAL_RUST_LIBS = "/org/apache/opendal/rust/libs";

    public static final String OPENDAL_JAVA = "opendal_java";

    static {
        JarJniLoader.loadLib(
                FileSystemOperator.class,
                ORG_APACHE_OPENDAL_RUST_LIBS,
                OPENDAL_JAVA);
    }

    private native long getOperator(String root);

    private native void freeOperator(long ptr);

    private native void write(long ptr, String fileName, String content);

    private native String read(long ptr, String fileName);

    private native void delete(long ptr, String fileName);

    public FileSystemOperator(String root) {
        this.root = root;
        this.ptr = getOperator(root);
    }

    @Override
    public void write(String fileName, String content) {
        write(this.ptr, fileName, content);
    }

    @Override
    public String read(String s) {
        return read(this.ptr, s);
    }

    @Override
    public void delete(String s) {
        delete(this.ptr, s);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.freeOperator(ptr);
    }
}
