// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Result;
use opendal::Operator;

/// All services should pass this test.
macro_rules! behavior_base_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _base>] {
                $(
                    #[test]
                    $(
                        #[$meta]
                    )*
                    fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(true);
                        match op {
                            Some(op)  => $crate::base::$test(op),
                            None => {
                                log::warn!("service {} not initiated, ignored", opendal::Scheme::$service);
                                Ok(())
                            }
                        }
                    }
                )*
            }
        }
    };
}

#[macro_export]
macro_rules! behavior_base_tests {
     ($($service:ident),*) => {
        $(
            behavior_base_test!(
                $service,

                test_metadata,
                test_object_id,
                test_object_path,
                test_object_name,

            );
        )*
    };
}

/// Create file with file path should succeed.
pub fn test_metadata(op: Operator) -> Result<()> {
    let _ = op.metadata();

    Ok(())
}

/// Test object id.
pub fn test_object_id(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let o = op.object(&path);

    assert_eq!(o.id(), format!("{}{}", op.metadata().root(), path));

    Ok(())
}

/// Test object path.
pub fn test_object_path(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let o = op.object(&path);

    assert_eq!(o.path(), path);

    Ok(())
}

/// Test object name.
pub fn test_object_name(op: Operator) -> Result<()> {
    // Normal
    let path = uuid::Uuid::new_v4().to_string();

    let o = op.object(&path);
    assert_eq!(o.name(), path);

    // File in subdir
    let name = uuid::Uuid::new_v4().to_string();
    let path = format!("{}/{}", uuid::Uuid::new_v4(), name);

    let o = op.object(&path);
    assert_eq!(o.name(), name);

    // Dir in subdir
    let name = uuid::Uuid::new_v4().to_string();
    let path = format!("{}/{}/", uuid::Uuid::new_v4(), name);

    let o = op.object(&path);
    assert_eq!(o.name(), format!("{name}/"));

    Ok(())
}
