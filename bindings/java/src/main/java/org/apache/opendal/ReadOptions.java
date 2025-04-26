package org.apache.opendal;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.nio.charset.Charset;

/**
 * Options for reading files. These options can be used to specify various parameters
 * when reading a file.
 */
@Builder
@Getter
public class ReadOptions {
    /**
     * The starting offset for reading. Defaults to 0, which means reading from the beginning of the file.
     */
    private final long offset = 0;

    /**
     * The number of bytes to read. If set to -1, it means reading all content from the offset to the end of the file.
     */
    private final long length = -1;

    /**
     * The buffer size used for reading. Defaults to 8192 bytes (8KB).
     */
    private final int bufferSize = 8192;
}
