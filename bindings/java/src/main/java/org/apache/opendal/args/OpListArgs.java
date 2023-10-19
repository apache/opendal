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

package org.apache.opendal.args;

import java.util.Objects;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OpListArgs {
    private String path;
    /**
     * The limit passed to underlying service to specify the max results that could return.
     */
    private long limit;
    /**
     * The start_after passes to underlying service to specify the specified key to start listing from.
     */
    private String startAfter;
    /**
     * The delimiter used to for the list operation. Default to be `/`
     */
    private String delimiter;
    /**
     * The metakey of the metadata that be queried. Default to be {@link Metakey#Mode}.
     */
    private int[] metakeys;

    public OpListArgs(String path) {
        this(path, -1, null, null, new int[] {});
    }

    public void setMetakeys(Metakey... metakeys) {
        this.metakeys = Stream.of(metakeys)
                .filter(Objects::nonNull)
                .mapToInt(Metakey::getId)
                .toArray();
    }

    /**
     * Metakey describes the metadata keys that can be queried.
     *
     * For query:
     *
     * At user side, we will allow user to query the metadata. If
     * the meta has been stored, we will return directly. If no, we will
     * call `stat` internally to fetch the metadata.
     */
    public enum Metakey {
        /**
         * The special metadata key that used to mark this entry
         * already contains all metadata.
         */
        Complete(0),
        /**
         *
         * Key for mode.
         */
        Mode(1),
        /**
         * Key for cache control.
         */
        CacheControl(2),
        /**
         * Key for content disposition.
         */
        ContentDisposition(3),
        /**
         * Key for content length.
         */
        ContentLength(4),
        /**
         * Key for content md5.
         */
        ContentMd5(5),
        /**
         * Key for content range.
         */
        ContentRange(6),
        /**
         * Key for content type.
         */
        ContentType(7),
        /**
         * Key for etag.
         */
        Etag(8),
        /**
         * Key for last last modified.
         */
        LastModified(9),
        /**
         * Key for version.
         */
        Version(10),
        ;

        private int id;

        Metakey(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }
}
