// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use jni::objects::JThrowable;
use jni::objects::JValue;
use jni::JNIEnv;
use opendal::ErrorKind;

pub(crate) struct Error {
    inner: opendal::Error,
}

impl Error {
    pub(crate) fn throw(&self, env: &mut JNIEnv) {
        if let Err(err) = self.do_throw(env) {
            match err {
                jni::errors::Error::JavaException => {
                    // other calls throws exception; safely ignored
                }
                _ => env.fatal_error(err.to_string()),
            }
        }
    }

    pub(crate) fn to_exception<'local>(
        &self,
        env: &mut JNIEnv<'local>,
    ) -> jni::errors::Result<JThrowable<'local>> {
        let class = env.find_class("org/apache/opendal/OpenDALException")?;
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
            ErrorKind::RangeNotSatisfied => "RangeNotSatisfied",
            _ => "Unexpected",
        })?;
        let message = env.new_string(format!("{:?}", self.inner))?;
        let exception = env.new_object(
            class,
            "(Ljava/lang/String;Ljava/lang/String;)V",
            &[JValue::Object(&code), JValue::Object(&message)],
        )?;
        Ok(JThrowable::from(exception))
    }

    fn do_throw(&self, env: &mut JNIEnv) -> jni::errors::Result<()> {
        let exception = self.to_exception(env)?;
        env.throw(exception)
    }
}

impl From<opendal::Error> for Error {
    fn from(err: opendal::Error) -> Self {
        Self { inner: err }
    }
}

impl From<jni::errors::Error> for Error {
    fn from(err: jni::errors::Error) -> Self {
        opendal::Error::new(ErrorKind::Unexpected, err.to_string()).into()
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
