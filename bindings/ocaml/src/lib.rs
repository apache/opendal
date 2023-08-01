use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use ::opendal as od;

mod block_operator;





pub fn new_operator( scheme_str: String,
    map: BTreeMap<String, String>,) -> Result<od::Operator,od::Error>{

    let hm:HashMap<String,String> = map.into_iter().collect();
    let schema: od::Scheme = od::Scheme::from_str(&scheme_str)?;
    od::Operator::via_map(schema, hm)
}



pub fn map_res_error<T>(res: Result<T,od::Error>)->Result<T,String>{
  res.map_err(|e|e.to_string())
}
