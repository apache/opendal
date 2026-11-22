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

import lombok.Builder;

/**
 * Options for deleting a path. All supplied conditions must hold for deletion to proceed.
 * An unsupported option fails with {@code Unsupported} and a false condition fails with
 * {@code ConditionNotMatch}. Optional reference fields default to null.
 */
@Builder
public class DeleteOptions {
    /**
     * Selects a stored version to delete. Requires service support for deleting versions.
     */
    public final String version;

    /**
     * Deletes the target recursively. Defaults to false.
     * Use a trailing slash to restrict deletion to the directory. On prefix-based services,
     * a path such as {@code "a"} can also match entries under {@code "ab/"}.
     */
    public final boolean recursive;

    /**
     * Deletes only when the current file has this exact ETag.
     * A missing file fails the condition. Only concrete ETags are portable.
     */
    public final String ifMatch;

    /**
     * Deletes only when the current file has a different ETag or is missing.
     * Only concrete ETags are portable.
     */
    public final String ifNoneMatch;

    /**
     * Deletes only when the current file has this exact version.
     * A missing file fails the condition.
     */
    public final String ifVersionMatch;

    /**
     * Deletes only when the current file has a different version.
     * The behavior for a missing file depends on the service.
     */
    public final String ifVersionNotMatch;

    /**
     * Deletes only when the current file still has the identity recorded in metadata
     * previously returned by OpenDAL for the same path.
     * OpenDAL uses a version match when the service supports it and a version is present,
     * or a non-weak ETag when available. Metadata with neither identity fails with
     * {@code ConfigInvalid}. A conflicting explicit match condition fails with
     * {@code ConditionNotMatch}.
     */
    public final Metadata ifNotChanged;
}
