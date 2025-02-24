#pragma once
#include <curl/curl.h>
#include <glog/logging.h>
#include "FileDecryptionProperties.h"
#include "InMemoryKMSClient.h"
#include "KeyRetriever.h"
#include "RangerKMSClient.h"
#include "common/base/Exceptions.h"

namespace facebook::velox::parquet {

class CryptoFactory {
 public:
  static void initialize(const std::string& kmsUri,
                         const std::string& keytabPath,
                         const bool clacEnabled) {
    instance_ = std::unique_ptr<CryptoFactory>(
        new CryptoFactory(kmsUri, keytabPath, clacEnabled));
  }
  static CryptoFactory& getInstance() {
    if (!instance_) {
      initialize("", "", false);
    }
    return *instance_;
  }

  DecryptionKeyRetriever& getDecryptionKeyRetriever() {
   return *kmsClient_;
  }

  std::shared_ptr<FileDecryptionProperties> getFileDecryptionProperties() {
    return FileDecryptionProperties::Builder().plaintextFilesAllowed()
        ->disableFooterSignatureVerification()
        ->keyRetriever(kmsClient_)
        ->build();
  }

  bool clacEnabled() { return clacEnabled_; }

  ~CryptoFactory() {
    curl_global_cleanup();
  }
 private:
  CryptoFactory(const std::string& kmsUri,
                const std::string& keytabPath,
                const bool clacEnabled) {
    curl_global_init(CURL_GLOBAL_ALL); // Initialize curl globally

    if (!kmsUri.empty() && !keytabPath.empty()) {
      if (setenv("KRB5_CLIENT_KTNAME", keytabPath.c_str(), 1) != 0) {
        VELOX_FAIL("failed to set KRB5_CLIENT_KTNAME: {}", keytabPath);
      }
      kmsClient_ = std::make_shared<RangerKMSClient>(kmsUri);
    } else {
      kmsClient_ = std::make_shared<InMemoryKMSClient>();
    }
    clacEnabled_ = clacEnabled;
  }
  std::shared_ptr<DecryptionKeyRetriever> kmsClient_;
  static std::unique_ptr<CryptoFactory> instance_;
  bool clacEnabled_;
};

}
