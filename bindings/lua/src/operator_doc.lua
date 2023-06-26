--- OpenDAL operator.
-- @classmod opendal.operator
-- @pragma nostrip

local _M = {}

--- Construct an operator based on scheme and option. Uses an table of key-value pairs to initialize 
--- the operator based on provided scheme and options. For each scheme, i.e. Backend, 
--- different options could be set, you may  reference the https://opendal.apache.org/docs/category/services/
--- for each service, especially for the **Configuration Part**.
-- @param string scheme the service scheme you want to specify, e.g. "fs", "s3", "supabase"
-- @param table options the table to the options for this operators
-- @return table, error  opendal operator table which contain opendal operator instance, error nil if sucess, others otherwise
-- @function new


--- Blockingly write raw bytes to path. 
---  Write the bytes into the path blockingly, returns nil if succeeds, others otherwise
-- @param string path the designated path you want to write your bytes in
-- @param string bytes the bytes to be written
-- @return error nil if success, otherwise error message
-- @function write

--- Blockingly read raw bytes from path. 
---  Read the data out from `path` blockingly by operator, returns the string if succeeds, others otherwise
-- @param string path the designated path you want to write your bytes in
-- @return string, error readed data, error nil if success, otherwise error message
-- @function read

--- Blockingly delete the object in path.
---  Delete the object in path blockingly, returns error nil
---  if succeeds, others otherwise
-- @param string path the designated path you want to write your delete
-- @return error error nil if success, otherwise error message
-- @function delete

--- Check whether the path exists.
-- @param string path the designated path you want to write your delete
-- @return bool, error true or false depend on operator instance and path, error nil if success, otherwise error message
-- @function is_exist

--- Stat the path, return its metadata.
-- @param string ,path the designated path you want to write your delete
-- @return table , error opendal.metadata instance table, error nil if success, otherwise error message
-- @function stat


return _M