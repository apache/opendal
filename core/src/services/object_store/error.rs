use crate::*;

pub(crate) fn parse_error(err: object_store::Error) -> Error {
    let err = match err {
        object_store::Error::NotFound { .. } => Error::new(ErrorKind::NotFound, "path not found"),

        object_store::Error::AlreadyExists { .. } => {
            Error::new(ErrorKind::AlreadyExists, "path already exists")
        }

        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => {
            Error::new(ErrorKind::PermissionDenied, "permission denied")
        }

        object_store::Error::InvalidPath { .. } => Error::new(ErrorKind::NotFound, "invalid path"),

        object_store::Error::NotSupported { .. } => {
            Error::new(ErrorKind::Unsupported, "operation not supported")
        }

        object_store::Error::Precondition { .. } => {
            Error::new(ErrorKind::ConditionNotMatch, "precondition not met")
        }

        object_store::Error::Generic { store, .. } => {
            Error::new(ErrorKind::Unexpected, format!("{} operation failed", store)).set_source(err)
        }

        _ => Error::new(ErrorKind::Unexpected, "unknown error").set_source(err),
    };

    err.with_context("service", Scheme::ObjectStore)
}
