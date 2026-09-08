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
 * Controls how an input stream executes reads, independently of its logical range.
 * Setting concurrent without setting chunk does not enable concurrent range reads.
 * Invalid values cause an OpenDALException with code ConfigInvalid when the stream is created.
 */
@Builder
public final class ReaderOptions {
    /**
     * Maximum number of internal chunk requests executed concurrently. Must be positive.
     * This is not the number of application transfers or Java threads and only affects chunked reads.
     */
    @Builder.Default
    public final int concurrent = 1;

    /**
     * Target size of each internal range request, in bytes. A positive value enables chunked reads.
     * The default of -1 keeps unchunked streaming. Zero and other negative values are invalid.
     * The value must fit the native platform's unsigned pointer-sized integer.
     */
    @Builder.Default
    public final long chunk = -1L;

    /**
     * Maximum number of completed chunks buffered ahead of consumption, not a byte count.
     * Must be non-negative. The default of zero applies strict backpressure.
     * This option only affects chunked reads.
     */
    @Builder.Default
    public final int prefetch = 0;

    /**
     * Known full object content length in bytes, independent of the requested range.
     * The default of -1 means unknown; zero is valid for an empty object.
     * This hint can avoid a metadata request and is not a consistency condition.
     * An incorrect hint can cause incomplete reads or errors. Values below -1 are invalid.
     */
    @Builder.Default
    public final long contentLengthHint = -1L;
}
