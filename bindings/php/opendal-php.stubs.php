<?php

// Stubs for opendal-php

namespace OpenDAL {
    class Metadata {
        public $content_disposition;

        public $content_md5;

        public $content_type;

        public $content_length;

        public $mode;

        public $etag;

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
        public $is_file;

        public $is_dir;

        public function is_dir(): int {}

        public function is_file(): int {}
    }

    class Operator {
        public function __construct(string $scheme_str, array $config) {}

        /**
         * Write string into given path.
         */
        public function write(string $path, string $content): mixed {}

        /**
         * Write bytes into given path, binary safe.
         */
        public function write_binary(string $path, array $content): mixed {}

        /**
         * Read the whole path into bytes, binary safe.
         */
        public function read(string $path): string {}

        /**
         * Check if this path exists or not, return 1 if exists, 0 otherwise.
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
}
