#pragma once

#include <string>
#include <vector>

namespace facebook::velox::parquet {

struct KeyMetadata {
  std::string name;
  std::string iv;
  int version;
  std::string eek;

  KeyMetadata() {}
  KeyMetadata(std::string name,
              std::string iv,
              int version,
              std::string& eek): name(name), iv(iv), version(version), eek(eek) {}
};

class KeyMetadataAssembler {
 public:
  static KeyMetadata unAssembly(const std::string& keyMetadata);
 private:

};

}
