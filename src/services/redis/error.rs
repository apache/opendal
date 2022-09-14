use crate::error::{other, ObjectError};
use crate::ops::Operation;
use anyhow::anyhow;
use redis::RedisError;
use std::io::Error;

pub(crate) fn new_get_async_con_error(err: RedisError, op: Operation, path: &str) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("getting async connection: {err:?}"),
    ))
}

pub(crate) fn new_serialize_metadata_error(
    err: bincode::Error,
    op: Operation,
    path: &str,
) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("serialize metadata: {err:?}"),
    ))
}

pub(crate) fn new_deserialize_metadata_error(
    err: bincode::Error,
    op: Operation,
    path: &str,
) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("deserialize metadata: {err:?}"),
    ))
}

pub(crate) fn new_exec_async_cmd_error(err: RedisError, op: Operation, path: &str) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("executing async command: {err:?}"),
    ))
}
