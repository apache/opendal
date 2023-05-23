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

package org.apache.opendal.exception;

public class ODException extends RuntimeException {
    private final Code code;

    @SuppressWarnings("unused") // called by jni-rs
    public ODException(String code, String message) {
        this(Code.valueOf(code), message);
    }

    public ODException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public Code getCode() {
        return code;
    }

    public enum Code {
        Unexpected,
        Unsupported,
        ConfigInvalid,
        NotFound,
        PermissionDenied,
        IsADirectory,
        NotADirectory,
        AlreadyExists,
        RateLimited,
        IsSameFile,
        ConditionNotMatch,
        ContentTruncated,
        ContentIncomplete,
    }
}
