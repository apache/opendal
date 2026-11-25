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

/**
 * Reports a Rust panic caught at a native method boundary or in an asynchronous OpenDAL operation.
 *
 * <p>A native method throws this exception directly. An operation that panics on the asynchronous
 * executor completes its future exceptionally with this exception as the cause.
 *
 * <p>Close the affected operator and its derived resources and create a fresh operator before
 * continuing. Check the state of storage before retrying, since the failed operation may have
 * already made changes.
 */
public final class OpenDALPanicException extends RuntimeException {
    OpenDALPanicException(String message) {
        super(message);
    }
}
