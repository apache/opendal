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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NativeObject is the base-class of all OpenDAL classes that have
 * a pointer to a native object.
 *
 * <p>
 * NativeObject has the {@link NativeObject#close()} method, which frees its associated
 * native object.
 *
 * <p>
 * This function should be called manually, or even better, called implicitly using a
 * <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">try-with-resources</a>
 * statement, when you are finished with the object. It is no longer called automatically
 * during the regular Java GC process via {@link NativeObject#finalize()}.
 *
 * <p>
 * <b>Explanatory note</b>
 *
 * <p>
 * When or if the Garbage Collector calls {@link Object#finalize()}
 * depends on the JVM implementation and system conditions, which the programmer
 * cannot control. In addition, the GC cannot see through the native reference
 * long member variable (which is the pointer value to the native object),
 * and cannot know what other resources depend on it.
 *
 * <p>
 * Finalization is deprecated and subject to removal in a future release.
 * The use of finalization can lead to problems with security, performance,
 * and reliability. See <a href="https://openjdk.org/jeps/421">JEP 421</a>
 * for discussion and alternatives.
 */
public abstract class NativeObject implements AutoCloseable {
    static {
        NativeLibrary.loadLibrary();
    }

    private final AtomicBoolean disposed = new AtomicBoolean(false);

    /**
     * An immutable reference to the value of the underneath pointer pointing
     * to some underlying native OpenDAL object.
     */
    protected final long nativeHandle;

    protected NativeObject(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    @Override
    public void close() {
        if (disposed.compareAndSet(false, true)) {
            disposeInternal(nativeHandle);
        }
    }

    /**
     * Check if the object has been disposed. Useful for defensive programming.
     *
     * @return if the object has been disposed.
     */
    public boolean isDisposed() {
        return disposed.get();
    }

    /**
     * Deletes underlying native object pointer.
     *
     * @param handle to the native object pointer
     */
    protected abstract void disposeInternal(long handle);
}
