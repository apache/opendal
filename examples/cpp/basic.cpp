#include "opendal.hpp"

#include <string>
#include <iostream>

int main() {
  std::string_view data = "abc";

  // Init operator
  opendal::Operator op = opendal::Operator("memory");

  // Write data to operator
  op.write("test", data);

  // Read data from operator
  auto res = op.read("test"); // res == data
  std::cout << res << std::endl;

  // Using reader
  auto reader = op.reader("test");
  std::string res2(3, 0);
  reader.read(res2.data(), data.size()); // res2 == "abc"
  std::cout << res2 << std::endl;

  // Using reader stream
  opendal::ReaderStream stream(op.reader("test"));
  std::string res3;
  stream >> res3; // res3 == "abc"
  std::cout << res3 << std::endl;
}
