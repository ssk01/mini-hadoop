#pragma once

#include <cstdint>
#include "io/buffer.h"

namespace mini_hadoop {

class Writable {
 public:
  virtual ~Writable() = default;
  virtual void Write(OutputBuffer& out) const = 0;
  virtual void ReadFields(InputBuffer& in) = 0;
};

class WritableComparable : public Writable {
 public:
  virtual int CompareTo(const WritableComparable& other) const = 0;

  bool operator<(const WritableComparable& other) const {
    return CompareTo(other) < 0;
  }
  bool operator==(const WritableComparable& other) const {
    return CompareTo(other) == 0;
  }
};

}  // namespace mini_hadoop
