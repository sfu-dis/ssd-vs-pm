#include <iostream>
#include <iomanip>
#include "utils.h"

void utils::printBytes(const unsigned char * ptr) {
  for (int i = 0; i < 8; i++, ptr++) {
    if (i % 8 == 0) {
      std::cout << std::endl;
    }
    std::cout << std::setw(2) << static_cast<unsigned>(*ptr) << " ";
  }
}