CREATE TABLE IF NOT EXISTS `demo` (
    `key` TEXT PRIMARY KEY NOT NULL CHECK(length(key) <= 255),
    `data` BLOB
);
