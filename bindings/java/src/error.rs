use jni::objects::{JThrowable, JValue};
use jni::JNIEnv;
use opendal::ErrorKind;
use std::fmt::{Debug, Display, Formatter};

pub(crate) struct Error {
    inner: opendal::Error,
}

impl Error {
    pub(crate) fn throw(&self, env: &mut JNIEnv) {
        if let Err(err) = self.do_throw(env) {
            env.fatal_error(err.to_string());
        }
    }

    fn do_throw(&self, env: &mut JNIEnv) -> jni::errors::Result<()> {
        let class = env.find_class("org/apache/opendal/exception/ODException")?;
        let code = env.new_string(match self.inner.kind() {
            ErrorKind::Unexpected => "Unexpected",
            ErrorKind::Unsupported => "Unsupported",
            ErrorKind::ConfigInvalid => "ConfigInvalid",
            ErrorKind::NotFound => "NotFound",
            ErrorKind::PermissionDenied => "PermissionDenied",
            ErrorKind::IsADirectory => "IsADirectory",
            ErrorKind::NotADirectory => "NotADirectory",
            ErrorKind::AlreadyExists => "AlreadyExists",
            ErrorKind::RateLimited => "RateLimited",
            ErrorKind::IsSameFile => "IsSameFile",
            ErrorKind::ConditionNotMatch => "ConditionNotMatch",
            ErrorKind::ContentTruncated => "ContentTruncated",
            ErrorKind::ContentIncomplete => "ContentIncomplete",
            _ => "Unexpected",
        })?;
        let message = env.new_string(self.inner.to_string())?;
        let exception = env.new_object(
            class,
            "(Ljava/lang/String;Ljava/lang/String;)V",
            &[JValue::Object(&code), JValue::Object(&message)],
        )?;
        env.throw(JThrowable::from(exception))
    }
}

impl From<opendal::Error> for Error {
    fn from(error: opendal::Error) -> Self {
        Self { inner: error }
    }
}

impl From<jni::errors::Error> for Error {
    fn from(error: jni::errors::Error) -> Self {
        Self {
            inner: opendal::Error::new(ErrorKind::Unexpected, &error.to_string()).set_source(error),
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(error: std::str::Utf8Error) -> Self {
        Self {
            inner: opendal::Error::new(ErrorKind::Unexpected, &error.to_string()).set_source(error),
        }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Self {
            inner: opendal::Error::new(ErrorKind::Unexpected, &error.to_string()).set_source(error),
        }
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.inner, f)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}
