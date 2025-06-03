package org.apache.opendal;

import java.time.Instant;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class StatOptions {

    /**
     * Sets if-match condition for this operation.
     * If file exists and its etag doesn't match, an error will be returned.
     */
    private String ifMatch;

    /**
     * Sets if-none-match condition for this operation.
     * If file exists and its etag matches, an error will be returned.
     */
    private String ifNoneMatch;

    /**
     * Sets if-modified-since condition for this operation.
     * If file exists and hasn't been modified since the specified time, an error will be returned.
     */
    private Instant ifModifiedSince;

    /**
     * Sets if-unmodified-since condition for this operation.
     * If file exists and has been modified since the specified time, an error will be returned.
     */
    private Instant ifUnmodifiedSince;

    /**
     * Sets version for this operation.
     * Retrieves data of a specified version of the given path.
     */
    private String version;

    /**
     * Specifies the content-type header for presigned operations.
     * Only meaningful when used along with presign.
     */
    private String overrideContentType;

    /**
     * Specifies the cache-control header for presigned operations.
     * Only meaningful when used along with presign.
     */
    private String overrideCacheControl;

    /**
     * Specifies the content-disposition header for presigned operations.
     * Only meaningful when used along with presign.
     */
    private String overrideContentDisposition;
}
