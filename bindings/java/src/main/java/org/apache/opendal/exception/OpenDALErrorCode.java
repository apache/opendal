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

public enum OpenDALErrorCode {
    UNEXPECTED,
    UNSUPPORTED,
    CONFIG_INVALID,
    NOT_FOUND,
    PERMISSION_DENIED,
    IS_A_DIRECTORY,
    NOT_A_DIRECTORY,
    ALREADY_EXISTS,
    RATE_LIMITED,
    IS_SAME_FILE,
    CONDITION_NOT_MATCH,
    CONTENT_TRUNCATED,
    CONTENT_INCOMPLETE;

    public static OpenDALErrorCode parse(String errorCode) {
        switch (errorCode) {
            case "Unsupported":
                return UNSUPPORTED;
            case "ConfigInvalid":
                return CONFIG_INVALID;
            case "NotFound":
                return NOT_FOUND;
            case "PermissionDenied":
                return PERMISSION_DENIED;
            case "IsADirectory":
                return IS_A_DIRECTORY;
            case "NotADirectory":
                return NOT_A_DIRECTORY;
            case "AlreadyExists":
                return ALREADY_EXISTS;
            case "RateLimited":
                return RATE_LIMITED;
            case "IsSameFile":
                return IS_SAME_FILE;
            case "ConditionNotMatch":
                return CONDITION_NOT_MATCH;
            case "ContentTruncated":
                return CONTENT_TRUNCATED;
            case "ContentIncomplete":
                return CONTENT_INCOMPLETE;
            default:
                return UNEXPECTED;
        }
    }
}
