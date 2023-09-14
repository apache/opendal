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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TestHelper {
    public static <T, U> BiFunction<T, Throwable, U> assertAsyncOpenDALException(OpenDALException.Code code) {
        return (result, throwable) -> {
            assertThat(result).isNull();
            final Throwable t = stripException(throwable, CompletionException.class);
            assertThat(t).isInstanceOf(OpenDALException.class);
            assertThat(((OpenDALException) t).getCode()).isEqualTo(code);
            return null;
        };
    }

    public static Throwable stripException(Throwable throwableToStrip, Class<? extends Throwable> typeToStrip) {
        while (typeToStrip.isAssignableFrom(throwableToStrip.getClass()) && throwableToStrip.getCause() != null) {
            throwableToStrip = throwableToStrip.getCause();
        }
        return throwableToStrip;
    }
}
