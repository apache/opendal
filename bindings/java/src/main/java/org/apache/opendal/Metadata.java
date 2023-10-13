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

import java.util.Date;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Metadata carries all metadata associated with a path.
 */
@ToString
@EqualsAndHashCode
public class Metadata {
    public final EntryMode mode;
    public final long contentLength;
    public final String contentDisposition;
    public final BytesContentRange contentRange;
    public final String contentMd5;
    public final String contentType;
    public final String cacheControl;
    public final String etag;
    public final Date lastModified;
    public final String version;

    public Metadata(
            int mode,
            long contentLength,
            String contentDisposition,
            BytesContentRange contentRange,
            String contentMd5,
            String contentType,
            String cacheControl,
            String etag,
            Date lastModified,
            String version) {
        this.mode = EntryMode.of(mode);
        this.contentLength = contentLength;
        this.contentDisposition = contentDisposition;
        this.contentRange = contentRange;
        this.contentMd5 = contentMd5;
        this.contentType = contentType;
        this.cacheControl = cacheControl;
        this.etag = etag;
        this.lastModified = lastModified;
        this.version = version;
    }

    public boolean isFile() {
        return mode == EntryMode.FILE;
    }

    public boolean isDir() {
        return mode == EntryMode.DIR;
    }

    public enum EntryMode {
        /// FILE means the path has data to read.
        FILE,
        /// DIR means the path can be listed.
        DIR,
        /// Unknown means we don't know what we can do on this path.
        UNKNOWN;

        public static EntryMode of(int mode) {
            switch (mode) {
                case 0:
                    return EntryMode.FILE;
                case 1:
                    return EntryMode.DIR;
                default:
                    return EntryMode.UNKNOWN;
            }
        }
    }

    @ToString
    @EqualsAndHashCode
    public static class BytesContentRange {
        /**
         * Start position of the range. `-1` means unknown.
         */
        public final long start;
        /**
         * End position of the range. `-1` means unknown.
         */
        public final long end;
        /**
         * Size of the whole content. `-1` means unknown.
         */
        public final long size;

        public BytesContentRange(long start, long end, long size) {
            this.start = start;
            this.end = end;
            this.size = size;
        }
    }
}
