--- OpenDAL metadata It must be returned operator::stat()
-- @classmod opendal.metadata
-- @pragma nostrip

local _M = {}

--- Return whether the path represents a file
-- @return bool if it is file, otherwise false
-- @function is_file

--- Return whether the path represents a directory
-- @return bool if it is directory, otherwise false
-- @function is_dir

--- Return the content_length of the metadata
-- @return bool if it is directory, otherwise false
-- @function content_length

return _M