// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.opendal;

import org.apache.commons.lang3.StringUtils;
import org.apache.opendal.exception.OpenDALErrorCode;
import org.apache.opendal.exception.OpenDALException;

public class Utils {
    public static void checkNullPointer(long ptr) {
        if (ptr == 0) {
            throw new OpenDALException(OpenDALErrorCode.UNEXPECTED, "null pointer found");
        }
    }

    public static void checkNotBlank(String argument, String argumentName) {
        if (StringUtils.isBlank(argument)) {
            throw new IllegalArgumentException("argument " + argumentName + "could not be null or blank");
        }
    }

    public static void checkNotNull(Object argument, String argumentName) {
        if (argument == null) {
            throw new IllegalArgumentException("argument " + argumentName + "could not be null");
        }
    }
}
