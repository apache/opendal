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

macro_rules! handle_opendal_result {
    ($env:expr, $result:expr) => {
        if let Err(error) = $result {
            let exception_result = $crate::util::opendal_error_into_exception(&mut $env, error);
            if let Ok(exception) = exception_result {
                let _ = $env.throw(exception);
            }
            return;
        } else {
            $result.unwrap()
        }
    };
    ($env:expr, $result:expr, $return_var:expr) => {
        if let Err(error) = $result {
            let exception_result = $crate::util::opendal_error_into_exception(&mut $env, error);
            if let Ok(exception) = exception_result {
                let _ = $env.throw(exception);
            }
            return $return_var;
        } else {
            $result.unwrap()
        }
    };
}

macro_rules! handle_jni_result {
    ($env:expr, $result:expr) => {
        if let Err(error) = $result {
            let exception_result =
                $crate::util::new_exception(&mut $env, "JNI", &error.to_string());
            if let Ok(exception) = exception_result {
                let _ = $env.throw(exception);
            }
            return;
        } else {
            $result.unwrap()
        }
    };
    ($env:expr, $result:expr, $return_var:expr) => {
        if let Err(error) = $result {
            let exception_result =
                $crate::util::new_exception(&mut $env, "JNI", &error.to_string());
            if let Ok(exception) = exception_result {
                let _ = $env.throw(exception);
            }
            return $return_var;
        } else {
            $result.unwrap()
        }
    };
}

pub(crate) use handle_jni_result;
pub(crate) use handle_opendal_result;
