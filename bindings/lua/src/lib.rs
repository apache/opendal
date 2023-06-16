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

use std::{collections::HashMap, str::FromStr};

use ::opendal as od;
use mlua::prelude::*;
use mlua::UserData;

#[derive(Clone)]
struct ODOperator {
    operator: od::BlockingOperator,
}

impl UserData for ODOperator {}

fn operator_new<'a>(
    lua: &'a Lua,
    (schema, option): (String, LuaTable<'a>),
) -> LuaResult<LuaTable<'a>> {
    if schema.is_empty() {
        return Err(LuaError::external("schema is empty"));
    }

    let mut map = HashMap::default();
    for pair in option.pairs::<String, String>() {
        let (key, value) = pair?;
        map.insert(key, value);
    }

    let od_schema = match od::Scheme::from_str(&schema) {
        Ok(s) => s,
        Err(e) => return Err(LuaError::external(e)),
    };

    let op = match od::Operator::via_map(od_schema, map) {
        Ok(o) => o.blocking(),
        Err(e) => return Err(LuaError::external(e)),
    };

    // this prevents the operator memory from being dropped by the Box
    let op = ODOperator { operator: op };

    let operator = lua.create_table()?;
    operator.set("_operator", op)?;
    operator.set("read", lua.create_function(operator_read)?)?;
    operator.set("write", lua.create_function(operator_write)?)?;
    operator.set("delete", lua.create_function(operator_delete)?)?;
    operator.set("is_exist", lua.create_function(operator_is_exist)?)?;
    operator.set("close", lua.create_function(operator_close)?)?;
    Ok(operator)
}

fn operator_is_exist<'a>(_: &'a Lua, (operator, path): (LuaTable<'a>, String)) -> LuaResult<bool> {
    let op = operator.get::<_, ODOperator>("_operator")?;
    let op = op.operator;

    let path = path.as_str();
    let res = op.is_exist(path);
    match res {
        Ok(exist) => Ok(exist),
        Err(e) => Err(LuaError::external(e)),
    }
}

fn operator_delete<'a>(_: &'a Lua, (operator, path): (LuaTable<'a>, String)) -> LuaResult<()> {
    let op = operator.get::<_, ODOperator>("_operator")?;
    let op = op.operator;

    let path = path.as_str();
    let res = op.delete(path);
    match res {
        Ok(_) => Ok(()),
        Err(e) => Err(LuaError::external(e)),
    }
}

fn operator_write<'a>(
    _: &'a Lua,
    (operator, path, bytes): (LuaTable<'a>, String, String),
) -> LuaResult<()> {
    let op = operator.get::<_, ODOperator>("_operator")?;
    let op = op.operator;

    let path = path.as_str();
    let res = op.write(path, bytes);
    match res {
        Ok(_) => Ok(()),
        Err(e) => Err(LuaError::external(e)),
    }
}

fn operator_read<'a>(
    lua: &'a Lua,
    (operator, path): (LuaTable<'a>, String),
) -> LuaResult<LuaString<'a>> {
    let op = operator.get::<_, ODOperator>("_operator")?;
    let op = op.operator;

    let path = path.as_str();
    let data = op.read(path);
    match data {
        Ok(data) => Ok(lua.create_string(&data)?),
        Err(e) => Err(LuaError::external(e)),
    }
}

fn operator_close<'a>(_: &'a Lua, operator: LuaTable<'a>) -> LuaResult<()> {
    operator.set("_operator", LuaNil)?;
    Ok(())
}

#[mlua::lua_module]
fn opendal(lua: &Lua) -> LuaResult<LuaTable> {
    let exports = lua.create_table()?;
    let operator = lua.create_table()?;
    operator.set("new", lua.create_function(operator_new)?)?;

    exports.set("operator", operator)?;

    Ok(exports)
}
