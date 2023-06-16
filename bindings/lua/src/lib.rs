use std::{collections::HashMap, str::FromStr};

use mlua::prelude::*;
use mlua::UserData;
use ::opendal as od;

#[derive(Clone)]
struct ODOperator {
    operator: od::BlockingOperator,
}

impl UserData for ODOperator {}

fn operator_new<'a>(lua: &'a Lua, (schema, option):  (String, LuaTable<'a>))-> LuaResult<LuaTable<'a>> {
    if schema.len() == 0 {
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

fn operator_is_exist<'a>(_: &'a Lua, (operator, path):  (LuaTable<'a>, String))-> LuaResult<bool> {
    let op = operator.get::<_, ODOperator>("_operator")?;
    let op = op.operator;

    let path = path.as_str();
    let res = op.is_exist(path);
    match res {
        Ok(exist) => Ok(exist),
        Err(e) => Err(LuaError::external(e)),
    }
}

// fn operator_stat<'a>(lua: &'a Lua, (operator, path):  (LuaTable<'a>, String))-> LuaResult<LuaTable<'a>> {
//     let op = operator.get::<_, ODOperator>("_operator")?;
//     let op = op.operator;

//     let path = path.as_str();
//     let res = op.stat(path);
//     match res {
//         Ok(meta) => {
//             let lua_meta = lua.create_table()?;
//             let entry = match meta.mode {
//                 Some(e) => e,
//                 None => return Err(LuaError::external("entry is empty")),
//             };

//             lua_meta.set("size", meta.size)?;
//             lua_meta.set("is_dir", meta.is_dir)?;
//             lua_meta.set("is_file", meta.is_file)?;
//             lua_meta.set("is_symlink", meta.is_symlink)?;
//             lua_meta.set("is_fifo", meta.is_fifo)?;
//             for (key, value) in meta {
//                 lua_meta.set(key, value)?;
//             }
//             Ok(lua_meta)
//         },
//         Err(e) => Err(LuaError::external(e)),
//     }
// }

fn operator_delete<'a>(_: &'a Lua, (operator, path):  (LuaTable<'a>, String))-> LuaResult<()> {
    let op = operator.get::<_, ODOperator>("_operator")?;
    let op = op.operator;

    let path = path.as_str();
    let res = op.delete(path);
    match res {
        Ok(_) => Ok(()),
        Err(e) => Err(LuaError::external(e)),
    }
}

fn operator_write<'a>(_: &'a Lua, (operator, path, bytes):  (LuaTable<'a>, String, String))-> LuaResult<()> {
    let op = operator.get::<_, ODOperator>("_operator")?;
    let op = op.operator;

    let path = path.as_str();
    let res = op.write(path, bytes);
    match res {
        Ok(_) => Ok(()),
        Err(e) => Err(LuaError::external(e)),
    }
}

fn operator_read<'a>(lua: &'a Lua, (operator, path):  (LuaTable<'a>, String))-> LuaResult<LuaString<'a>> {
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


fn hello(_: &Lua, name: String) -> LuaResult<()> {
    println!("hello, {}!", name);
    Ok(())
}


#[mlua::lua_module]
fn opendal(lua: &Lua) -> LuaResult<LuaTable> {
    let exports = lua.create_table()?;
    let operator = lua.create_table()?;
    operator.set("new", lua.create_function(operator_new)?)?;

    exports.set("operator", operator)?;
    exports.set("hello", lua.create_function(hello)?)?;

    Ok(exports)
}