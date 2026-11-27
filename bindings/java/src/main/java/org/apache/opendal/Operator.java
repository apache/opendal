/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.opendal;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Operator represents an underneath OpenDAL operator that accesses data
 * synchronously.
 *
 * <p>A Rust panic caught at a native method boundary is thrown as an {@link OpenDALPanicException}.</p>
 */
public class Operator extends NativeObject {
    public final OperatorInfo info;

    /**
     * Construct an OpenDAL blocking operator.
     *
     * @param config the config of the underneath service to access data from.
     */
    public static Operator of(ServiceConfig config) {
        try (final AsyncOperator operator = AsyncOperator.of(config)) {
            return operator.blocking();
        }
    }

    /**
     * Construct an OpenDAL blocking operator:
     *
     * <p>
     * You can find all possible schemes
     * <a href="https://docs.rs/opendal/latest/opendal/enum.Scheme.html">here</a>
     * and see what config options each service supports.
     *
     * @param scheme the name of the underneath service to access data from.
     * @param map    a map of properties to construct the underneath operator.
     */
    public static Operator of(String scheme, Map<String, String> map) {
        try (final AsyncOperator operator = AsyncOperator.of(scheme, map)) {
            return operator.blocking();
        }
    }

    Operator(long nativeHandle, OperatorInfo info) {
        super(nativeHandle);
        this.info = info;
    }

    /**
     * @return the cloned blocking operator.
     * @see AsyncOperator#duplicate()
     */
    public Operator duplicate() {
        final long nativeHandle = withHandle(Operator::duplicate);
        return new Operator(nativeHandle, this.info);
    }

    public void write(String path, String content) {
        write(path, content.getBytes(StandardCharsets.UTF_8));
    }

    public void write(String path, byte[] content) {
        write(path, content, WriteOptions.builder().build());
    }

    public void write(String path, String content, WriteOptions options) {
        write(path, content.getBytes(StandardCharsets.UTF_8), options);
    }

    public void write(String path, byte[] content, WriteOptions options) {
        useHandle(handle -> write(handle, path, content, options));
    }

    public OperatorOutputStream createOutputStream(String path) {
        return new OperatorOutputStream(this, path, WriteOptions.builder().build());
    }

    public OperatorOutputStream createOutputStream(String path, int maxBytes) {
        return new OperatorOutputStream(
                this, path, maxBytes, WriteOptions.builder().build());
    }

    public OperatorOutputStream createOutputStream(String path, WriteOptions options) {
        return new OperatorOutputStream(this, path, options);
    }

    public byte[] read(String path) {
        return read(path, ReadOptions.builder().build());
    }

    public byte[] read(String path, long offset, long length) {
        return read(path, ReadOptions.builder().offset(offset).length(length).build());
    }

    public byte[] read(String path, ReadOptions options) {
        return withHandle(handle -> read(handle, path, options));
    }

    /**
     * Creates a reusable reader for a file with default execution options.
     *
     * @param path file path
     * @return a reader that the caller must close
     * @see #createReader(String, ReaderOptions)
     */
    public OperatorReader createReader(String path) {
        return createReader(path, ReaderOptions.builder().build());
    }

    /**
     * Creates a reusable reader for a file. Options apply to every read through the reader;
     * each read selects its own byte range. The reader can outlive this operator.
     * Creation does not read file contents or guarantee that the file exists.
     * Version selection and conditions apply to all requests through the reader, including
     * streams. All conditions must hold; a failed condition on an existing file
     * fails with ConditionNotMatch and a missing file fails with NotFound. Depending on the
     * service, errors may surface at creation or during reading.
     *
     * @param path file path
     * @param options reader execution options
     * @return a reader that the caller must close
     * @throws OpenDALException if options are invalid (ConfigInvalid), the path is a directory
     *     (IsADirectory), or the service does not support reads or a requested option (Unsupported)
     * @throws IllegalStateException if this operator is closed
     */
    public OperatorReader createReader(String path, ReaderOptions options) {
        final long reader = withHandle(handle -> {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(options, "options");
            return createReader(handle, path, options);
        });
        return new OperatorReader(reader);
    }

    private static native long createReader(long operator, String path, ReaderOptions options);

    public OperatorInputStream createInputStream(String path) {
        return createInputStream(path, ReadOptions.builder().build());
    }

    /**
     * Opens a stream using the supplied read options.
     *
     * @param path the path to read
     * @param options the read options
     * @return the input stream
     * @throws OpenDALException if a read limit is set or the read options are invalid or unsupported
     */
    public OperatorInputStream createInputStream(String path, ReadOptions options) {
        return new OperatorInputStream(this, path, options);
    }

    public void delete(String path) {
        delete(path, DeleteOptions.builder().build());
    }

    /**
     * Deletes the given path with the supplied options. A missing file is a successful
     * no-op when all conditions hold. For recursive directory deletion, include the
     * trailing slash in the path.
     *
     * @param path the path to delete
     * @param options the delete options
     * @throws OpenDALException if an option is unsupported, a condition fails, or deletion fails
     */
    public void delete(String path, DeleteOptions options) {
        useHandle(handle -> delete(handle, path, options));
    }

    public Metadata stat(String path) {
        return stat(path, StatOptions.builder().build());
    }

    public Metadata stat(String path, StatOptions options) {
        return withHandle(handle -> stat(handle, path, options));
    }

    public void createDir(String path) {
        useHandle(handle -> createDir(handle, path));
    }

    public void copy(String sourcePath, String targetPath) {
        useHandle(handle -> copy(handle, sourcePath, targetPath));
    }

    public void rename(String sourcePath, String targetPath) {
        useHandle(handle -> rename(handle, sourcePath, targetPath));
    }

    public void removeAll(String path) {
        useHandle(handle -> removeAll(handle, path));
    }

    public List<Entry> list(String path) {
        return list(path, ListOptions.builder().build());
    }

    public List<Entry> list(String path, ListOptions options) {
        final Entry[] entries = withHandle(handle -> list(handle, path, options));
        return Arrays.asList(entries);
    }

    @Override
    protected native void disposeInternal(long handle);

    private static native long duplicate(long op);

    private static native void write(long op, String path, byte[] content, WriteOptions options);

    private static native byte[] read(long op, String path, ReadOptions options);

    private static native void delete(long op, String path, DeleteOptions options);

    private static native Metadata stat(long op, String path, StatOptions options);

    private static native void createDir(long op, String path);

    private static native void copy(long op, String sourcePath, String targetPath);

    private static native void rename(long op, String sourcePath, String targetPath);

    private static native void removeAll(long op, String path);

    private static native Entry[] list(long op, String path, ListOptions options);
}
