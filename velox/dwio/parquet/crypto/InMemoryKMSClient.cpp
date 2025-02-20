#include "InMemoryKMSClient.h"
#include "Exception.h"

namespace facebook::velox::parquet {

std::string InMemoryKMSClient::getKey(const std::string& keyMetadata, bool legalRead, const std::string& doAs) {
  //TODO legalRead true
  auto it = keyMap_.find(keyMetadata);
  if (it != keyMap_.end()) {
    return it->second;
  } else {
    throw CryptoException("[CLAC] http status code 403");
  }
}

std::string InMemoryKMSClient::getKey(const std::string& keyMetadata, const std::string& doAs) {
  return getKey(keyMetadata, false, doAs);
}

void InMemoryKMSClient::putKey(const std::string& keyMetadata, const std::string& key) {
  keyMap_[keyMetadata] = key;
}

}
