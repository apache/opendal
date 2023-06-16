local opendal = require("opendal")


local op, err = opendal.operator.new("memory",{root="/tmp"})
if err ~= nil then
    print(err)
    return
end
op:write("/tmp/test.txt","hello world")
print(op:read("/tmp/test.txt"))
op:close()