package org.apache.opendal.exception;

public class OpenDALException extends RuntimeException {
    private final OpenDALErrorCode errorCode;

    public OpenDALException(OpenDALErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public OpenDALErrorCode getErrorCode() {
        return errorCode;
    }
}
