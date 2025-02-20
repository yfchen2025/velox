#pragma once

#include <memory>
#include <string>
#include "velox/dwio/parquet/crypto/Cache.h"
#include "velox/dwio/parquet/crypto/KeyRetriever.h"
#include "EncryptionKey.h"

namespace facebook::velox::parquet {

constexpr int kCacheCleanupInternalSeconds = 60;

class RangerKMSClient : public DecryptionKeyRetriever {
 public:
  // takes url of format kms://http@localhost:18028/kms
  explicit RangerKMSClient(const std::string& kmsUri) {
    parseKmsUri(kmsUri);
  }

  std::string getKey(const std::string& keyMetadata, bool legalRead, const std::string& doAs) override;
  std::string getKey(const std::string& keyMetadata, const std::string& doAs) override;

 private:
  std::string getDecryptedKey(std::shared_ptr<EncryptedKeyVersion>& encryptedKeyVersion, const std::string& doAs);
  bool isLegalHoldKey(std::string& keyName);
  KeyVersion decryptEncryptedKey(std::shared_ptr<EncryptedKeyVersion>& encryptedKeyVersion, const std::string& doAs);
  void parseKmsUri(const std::string& uri);
  KeyVersion decrypt(std::shared_ptr<EncryptedKeyVersion>& encryptedKeyVersion, const std::string& doAs);

  static const std::string ARCHIVE_KEY_PREFIX_;
  std::string kmsUrl_;
  Cache cache_{kCacheCleanupInternalSeconds};
  Cache exceptionCache_{kCacheCleanupInternalSeconds};
};

}
