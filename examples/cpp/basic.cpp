#include "opendal.hpp"

#include <iostream>
#include <string>

int main() {
  std::string_view data = "abc";

  // Init operator
  opendal::Operator op = opendal::Operator("memory");

  // Write data to operator
  op.Write("test", data);

  // Read data from operator
  auto res = op.Read("test"); // res == data
  std::cout << res << std::endl;

  // Using reader
  auto reader = op.GetReader("test");
  std::string res2(3, 0);
  reader.Read(res2.data(), data.size()); // res2 == "abc"
  std::cout << res2 << std::endl;

  // Using reader stream
  opendal::ReaderStream stream(op.GetReader("test"));
  std::string res3;
  stream >> res3; // res3 == "abc"
  std::cout << res3 << std::endl;
}
