//
//  counter_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_COUNTER_GENERATOR_H_
#define YCSB_C_COUNTER_GENERATOR_H_

#include "generator.h"

#include <cstdint>
#include <atomic>

namespace ycsbc {

class CounterGenerator : public Generator<uint64_t> {
 public:
  CounterGenerator(uint64_t start) : counter_(start) { }
  uint64_t Next() override { return ++counter_; }
  uint64_t Last() override { return counter_ - 1; }
  void Set(uint64_t start) { counter_ = start; }
 private:
  uint64_t counter_;
};

} // ycsbc

#endif // YCSB_C_COUNTER_GENERATOR_H_
