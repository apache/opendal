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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
public class OpList<T> {
    private final String path;
    /**
     * The limit passed to underlying service to specify the max results that could return.
     */
    private final long limit;
    /**
     * The start_after passes to underlying service to specify the specified key to start listing from.
     */
    private final Optional<String> startAfter;
    /**
     * The delimiter used to for the list operation. Default to be `/`
     */
    private final Optional<String> delimiter;
    /**
     * The metakey of the metadata that be queried. Default to be {@link Metakey#Mode}.
     */
    private final Set<Metakey> metakeys;
    /**
     * The list function.
     */
    private final Function<OpList<T>, T> listFunc;

    /**
     * Calls the list function and returns the result.
     *
     * @return the result of calling the list
     */
    public T call() {
        return listFunc.apply(this);
    }

    private OpList(
            @NonNull String path,
            @NonNull Function<OpList<T>, T> listFunc,
            long limit,
            Optional<String> startAfter,
            Optional<String> delimiter,
            Set<Metakey> metakeys) {
        this.path = path;
        this.limit = limit;
        this.startAfter = startAfter;
        this.delimiter = delimiter;
        this.listFunc = listFunc;
        this.metakeys = metakeys;
    }

    public static <T> OpListBuilder<T> builder(String path, Function<OpList<T>, T> listFunc) {
        return new OpListBuilder<>(path, listFunc);
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

        private final int id;

        Metakey(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }

    @ToString
    public static class OpListBuilder<T> {
        private final String path;
        private final Function<OpList<T>, T> listFunc;

        private long limit;
        private Optional<String> startAfter;
        private Optional<String> delimiter;
        private Set<Metakey> metakeys;

        private OpListBuilder(String path, @NonNull Function<OpList<T>, T> listFunc) {
            this.path = path;
            this.listFunc = listFunc;
            this.limit = -1L;
            this.startAfter = Optional.empty();
            this.delimiter = Optional.empty();
            this.metakeys = new HashSet<>();
        }

        /**
         * Change the limit of this list operation.
         * @param limit the delimiter to be set
         * @return      the updated {@link OpListBuilder} object
         */
        public OpListBuilder<T> limit(long limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Change the start_after of this list operation.
         *
         * @param  startAfter  the value to set the "startAfter" parameter to
         * @return             the updated {@link OpListBuilder} object
         */
        public OpListBuilder<T> startAfter(String startAfter) {
            this.startAfter = Optional.ofNullable(startAfter);
            return this;
        }
        /**
         * Change the delimiter. The default delimiter is "/"
         *
         * @param  delimiter		the delimiter to be set
         * @return         		    the updated {@link OpListBuilder} object
         */
        public OpListBuilder<T> delimiter(String delimiter) {
            this.delimiter = Optional.ofNullable(delimiter);
            return this;
        }

        /**
         * Change the metakey of this list operation. The default metakey is `Metakey::Mode`.
         *
         * @param metakeys the metakeys to be set
         * @return         the updated {@link OpListBuilder} object
         */
        public OpListBuilder<T> metakeys(Metakey... metakeys) {
            this.metakeys.addAll(Arrays.asList(metakeys));
            return this;
        }

        /**
         * Build the OpList object with the provided parameters.
         *
         * @return        	The newly created {@link OpList} object.
         */
        public OpList<T> build() {
            return new OpList<>(path, listFunc, limit, startAfter, delimiter, metakeys);
        }
    }
}
