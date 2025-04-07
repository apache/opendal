#include "opendal.hpp"

#include <string>
#include <vector>
#include <iostream>

int main() {
  std::vector<uint8_t> data = {'a', 'b', 'c'};

  // Init operator
  opendal::Operator op = opendal::Operator("memory");

  // Write data to operator
  op.write("test", data);

  // Read data from operator
  auto res = op.read("test"); // res == data

  // Using reader
  auto reader = op.reader("test");
  opendal::ReaderStream stream(std::move(reader));
  std::string res2;
  stream >> res2; // res2 == "abc"
  std::cout << res2 << std::endl;
}
