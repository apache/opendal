package org.apache.opendal.exception;

public enum OpenDALErrorCode {
    UNEXPECTED,
    UNSUPPORTED,
    CONFIG_INVALID,
    NOT_FOUND,
    PERMISSION_DENIED,
    IS_A_DIRECTORY,
    NOT_A_DIRECTORY,
    ALREADY_EXISTS,
    RATE_LIMITED,
    IS_SAME_FILE,
    CONDITION_NOT_MATCH,
    CONTENT_TRUNCATED,
    CONTENT_INCOMPLETE;

    public static OpenDALErrorCode parse(String errorCode) {
        switch (errorCode) {
            case "Unsupported":
                return UNSUPPORTED;
            case "ConfigInvalid":
                return CONFIG_INVALID;
            case "NotFound":
                return NOT_FOUND;
            case "PermissionDenied":
                return PERMISSION_DENIED;
            case "IsADirectory":
                return IS_A_DIRECTORY;
            case "NotADirectory":
                return NOT_A_DIRECTORY;
            case "AlreadyExists":
                return ALREADY_EXISTS;
            case "RateLimited":
                return RATE_LIMITED;
            case "IsSameFile":
                return IS_SAME_FILE;
            case "ConditionNotMatch":
                return CONDITION_NOT_MATCH;
            case "ContentTruncated":
                return CONTENT_TRUNCATED;
            case "ContentIncomplete":
                return CONTENT_INCOMPLETE;
            default:
                return UNEXPECTED;
        }
    }
}
