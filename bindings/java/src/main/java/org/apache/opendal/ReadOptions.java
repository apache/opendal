package org.apache.opendal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;


/**
 * Options for reading files. These options can be used to specify various parameters
 * when reading a file.
 */

public class ReadOptions {
    /**
     * The starting offset for reading. Defaults to 0, which means reading from the beginning of the file.
     */
    private final long offset = 0;

    /**
     * The number of bytes to read. If set to -1, it means reading all content from the offset to the end of the file.
     */
    private final long length = -1;

    public ReadOptions() {
    }
    public long setOffset(long offset) {
        return offset;
    }

    public long setLength(long length) {
        return length;
    }
    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "ReadOptions{" +
            "offset=" + offset +
            ", length=" + length +
            '}';
    }
}
