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

package org.apache.opendal.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Layer;
import org.apache.opendal.NativeLibrary;
import org.apache.opendal.OpenDALPanicException;
import org.apache.opendal.Operator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named = "opendal.test-native-panics", matches = "true")
class NativePanicTest {
    @Test
    void nativeMethodThrowsAfterPanic() {
        NativeLibrary.loadLibrary();
        final OpenDALPanicException error =
                assertThrows(OpenDALPanicException.class, NativePanicTest::panicInNativeMethod);
        assertThat(error).hasMessage("panic in native method");
        assertFreshOperatorWorks();
    }

    @Test
    void asyncStatCompletesExceptionallyAfterNativePanic() {
        try (AsyncOperator base = AsyncOperator.of("memory", Collections.emptyMap());
                AsyncOperator panicking = withPanicOnStat(base)) {
            final ExecutionException error = assertThrows(
                    ExecutionException.class, () -> panicking.stat("object").get(5, TimeUnit.SECONDS));
            assertThat(error).hasCauseExactlyInstanceOf(OpenDALPanicException.class);
            assertThat(error.getCause()).hasMessage("panic during stat");
        }
        assertFreshOperatorWorks();
    }

    private static AsyncOperator withPanicOnStat(AsyncOperator base) {
        return base.layer(new Layer() {
            @Override
            protected long layer(long nativeOp) {
                return panicOnStat(nativeOp);
            }
        });
    }

    private static void assertFreshOperatorWorks() {
        try (Operator fresh = Operator.of("memory", Collections.emptyMap())) {
            fresh.write("object", new byte[] {1});
            assertThat(fresh.read("object")).containsExactly(1);
        }
    }

    private static native void panicInNativeMethod();

    private static native long panicOnStat(long nativeOp);
}
