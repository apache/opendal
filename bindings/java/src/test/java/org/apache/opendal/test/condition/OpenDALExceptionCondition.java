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

package org.apache.opendal.test.condition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import org.apache.opendal.OpenDALException;
import org.assertj.core.api.Condition;

public class OpenDALExceptionCondition extends Condition<Throwable> {
    private final List<Class<? extends Throwable>> canBeStripped;
    private final OpenDALException.Code code;

    public static OpenDALExceptionCondition ofAsync(OpenDALException.Code code) {
        final List<Class<? extends Throwable>> canBeStripped = new ArrayList<>();
        canBeStripped.add(CompletionException.class);
        canBeStripped.add(ExecutionException.class);
        return new OpenDALExceptionCondition(code, canBeStripped);
    }

    public static OpenDALExceptionCondition ofSync(OpenDALException.Code code) {
        return new OpenDALExceptionCondition(code, Collections.emptyList());
    }

    public OpenDALExceptionCondition(OpenDALException.Code code, List<Class<? extends Throwable>> canBeStripped) {
        as("OpenDALException with code " + code + ", stripping " + canBeStripped);
        this.code = code;
        this.canBeStripped = canBeStripped;
    }

    private Throwable stripException(Throwable throwable) {
        while (throwable.getCause() != null) {
            final Class<?> thisClass = throwable.getClass();
            final Predicate<Class<? extends Throwable>> predicate = clazz -> clazz.isAssignableFrom(thisClass);
            if (this.canBeStripped.stream().noneMatch(predicate)) {
                break;
            }
            throwable = throwable.getCause();
        }
        return throwable;
    }

    @Override
    public boolean matches(Throwable throwable) {
        throwable = stripException(throwable);

        if (throwable instanceof OpenDALException) {
            final OpenDALException exception = (OpenDALException) throwable;
            return exception.getCode() == code;
        }

        return false;
    }
}
