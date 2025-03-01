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

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class WriteOptions {

    /**
     * Sets the Content-Type header for the object.
     * Requires capability: writeWithContentType
     */
    private String contentType;

    /**
     * Sets the Content-Disposition header for the object
     * Requires capability: writeWithContentDisposition
     */
    private String contentDisposition;

    /**
     * Sets the Cache-Control header for the object
     * Requires capability: writeWithCacheControl
     */
    private String cacheControl;

    /**
     * Sets the Content-Encoding header for the object
     */
    private String contentEncoding;

    /**
     * Sets the If-Match header for conditional writes
     * Requires capability: writeWithIfMatch
     */
    private String ifMatch;

    /**
     * Sets the If-None-Match header for conditional writes
     * Requires capability: writeWithIfNoneMatch
     */
    private String ifNoneMatch;

    /**
     * Sets custom metadata for the file.
     * Requires capability: writeWithUserMetadata
     */
    private Map<String, String> userMetadata;

    /**
     * Enables append mode for writing.
     * When true, data will be appended to the end of existing file.
     * Requires capability: writeCanAppend
     */
    private boolean append;

    /**
     * Write only if the file does not exist.
     * Operation will fail if the file at the designated path already exists.
     * Requires capability: writeWithIfNotExists
     */
    private boolean ifNotExists;
}
