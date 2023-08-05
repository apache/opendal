<?php
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

// Stubs for opendal-php

namespace OpenDAL {
    class Operator {
        public function __construct(string $scheme_str, array $config) {}

        /**
         * Write bytes into given path.
         */
        public function write(string $path, string $content): mixed {}

        /**
         * Read the whole path into bytes.
         */
        public function read(string $path): string {}

        /**
         * Check if this path exists or not.
         */
        public function is_exist(string $path): int {}

        /**
         * Get current path's metadata **without cache** directly.
         *
         * # Notes
         *
         * Use `stat` if you:
         *
         * - Want detect the outside changes of path.
         * - Don't want to read from cached metadata.
         */
        public function stat(string $path): \OpenDAL\Metadata {}

        /**
         * Delete given path.
         *
         * # Notes
         *
         * - Delete not existing error won't return errors.
         */
        public function delete(string $path): mixed {}

        /**
         * Create a dir at given path.
         *
         * # Notes
         *
         * To indicate that a path is a directory, it is compulsory to include
         * a trailing / in the path. Failure to do so may result in
         * `NotADirectory` error being returned by OpenDAL.
         *
         * # Behavior
         *
         * - Create on existing dir will succeed.
         * - Create dir is always recursive, works like `mkdir -p`
         */
        public function create_dir(string $path): mixed {}
    }

    class Metadata {
        public $mode;

        public $content_type;

        public $etag;

        public $content_md5;

        public $content_disposition;

        public $content_length;

        public function content_disposition(): ?string {}

        /**
         * Content length of this entry.
         */
        public function content_length(): int {}

        /**
         * Content MD5 of this entry.
         */
        public function content_md5(): ?string {}

        /**
         * Content Type of this entry.
         */
        public function content_type(): ?string {}

        /**
         * ETag of this entry.
         */
        public function etag(): ?string {}

        /**
         * mode represent this entry's mode.
         */
        public function mode(): \OpenDAL\EntryMode {}
    }

    class EntryMode {
        public $is_dir;

        public $is_file;

        public function is_dir(): int {}

        public function is_file(): int {}
    }
}
