use super::*;

#[ocaml::sig] 
pub struct BlockingOperator(od::BlockingOperator);
ocaml::custom!(BlockingOperator);


#[ocaml::func]
#[ocaml::sig("string -> (string * string) list -> (blocking_operator, string) Result.t ")]
pub fn new_blocking_operator( scheme_str: String,
    map: BTreeMap<String, String>,) -> Result<ocaml::Pointer<BlockingOperator>,String>{
    let op = map_res_error(new_operator(scheme_str, map))?;
    Ok(BlockingOperator(op.blocking()).into())
}


#[ocaml::func]
#[ocaml::sig("blocking_operator -> string -> (bool, string) Result.t ")]
pub fn blocking_is_exist(operator: &mut BlockingOperator, path: String) -> Result<bool,String> {
    map_res_error(operator.0.is_exist(path.as_str()))
}

#[ocaml::func]
#[ocaml::sig("blocking_operator -> string -> (bool, string) Result.t ")]
pub fn blocking_create_dir(operator: &mut BlockingOperator, path: String) -> Result<(),String> {
    map_res_error(operator.0.create_dir(path.as_str()))
}

#[ocaml::func]
#[ocaml::sig("blocking_operator -> string -> (char array, string) Result.t ")]
pub fn blocking_read(operator: &mut BlockingOperator, path: String) -> Result< Vec<u8>,String> {
    map_res_error(operator.0.read(path.as_str()))
}


#[ocaml::func]
#[ocaml::sig("blocking_operator -> string -> bytes -> (unit, string) Result.t ")]
pub fn blocking_write(operator: &mut BlockingOperator, path: String, bs: & 'static [u8]) -> Result<(),String> {
    map_res_error(operator.0.write(path.as_str(), bs))
}


#[ocaml::func]
#[ocaml::sig("blocking_operator -> string -> string -> (unit, string) Result.t ")]
pub fn blocking_copy(operator: &mut BlockingOperator, from: String, to: String) -> Result<(),String> {
    map_res_error(operator.0.copy(from.as_str(), to.as_str()))
}

#[ocaml::func]
#[ocaml::sig("blocking_operator -> string -> string -> (unit, string) Result.t ")]
pub fn blocking_rename(operator: &mut BlockingOperator, from: String, to: String) -> Result<(),String> {
    map_res_error(operator.0.rename(from.as_str(), to.as_str()))
}

#[ocaml::func]
#[ocaml::sig("blocking_operator -> string -> (unit, string) Result.t ")]
pub fn blocking_delete(operator: &mut BlockingOperator, path: String) -> Result<(),String> {
    map_res_error(operator.0.delete(path.as_str()))
}

#[ocaml::func]
#[ocaml::sig("blocking_operator -> string array -> (unit, string) Result.t ")]
pub fn blocking_remove(operator: &mut BlockingOperator, path: Vec<String>) -> Result<(),String> {
    map_res_error(operator.0.remove(path))
}

#[ocaml::func]
#[ocaml::sig("blocking_operator -> string -> (unit, string) Result.t ")]
pub fn blocking_remove_all(operator: &mut BlockingOperator, path: String) -> Result<(),String> {
    map_res_error(operator.0.remove_all(path.as_str()))
}
