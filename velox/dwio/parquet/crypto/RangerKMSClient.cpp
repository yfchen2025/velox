#include "RangerKMSClient.h"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <map>
#include "KeyMetadataAssembler.h"
#include "PapyrusHelper.h"
#include "common/base/Exceptions.h"
#include "common/encode/Base64.h"
#include <glog/logging.h>
#include "Exception.h"
#include <functional>
#include <cstddef>
#include "Utils.h"

namespace facebook::velox::parquet {

// Callback function to handle the response data
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

// Define how to convert a JSON object to a KeyVersion struct
KeyVersion fromJson(std::string& jsonString) {
  nlohmann::json j = nlohmann::json::parse(jsonString);
  KeyVersion kv;
  j.at("name").get_to(kv.name);
  j.at("versionName").get_to(kv.versionName);

  std::string material;
  j.at("material").get_to(material);
  kv.material = velox::encoding::Base64::decodeUrl(material);

  return kv;
}

KeyVersion RangerKMSClient::decrypt(std::shared_ptr<EncryptedKeyVersion>& encryptedKeyVersion, const std::string& doAs) {
  std::string readBuffer;

  CURL* curl = curl_easy_init();
  // Set the URL for the request
  std::string url = kmsUrl_ + "keyversion/" + std::string(curl_easy_escape(curl, encryptedKeyVersion->encryptionKeyVersionName.c_str(), 0)) + "/_eek?eek_op=decrypt";
  if (!doAs.empty()) {
    url += "&doAs=" + std::string(curl_easy_escape(curl, doAs.c_str(), 0));
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

  // Set the HTTP method to POST
  curl_easy_setopt(curl, CURLOPT_POST, 1L);

  // The JSON payload
  std::map<std::string, std::string> payload;
  payload["name"] = encryptedKeyVersion->encryptionKeyName;
  payload["iv"] = velox::encoding::Base64::encode(encryptedKeyVersion->encryptedKeyIv);
  payload["material"] = velox::encoding::Base64::encode(encryptedKeyVersion->encryptedKeyVersion->material);
  nlohmann::json j = payload;
  std::string jsonString = j.dump();

  // Set the POST fields (JSON payload)
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonString.c_str());

  // Specify the content type as JSON
  struct curl_slist *headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  // Set the write callback function to capture the response
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

  // Set CURL options for Kerberos
  curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_GSSNEGOTIATE);
  curl_easy_setopt(curl, CURLOPT_USERPWD, ":"); // Use a blank user password to trigger GSS-API authentication

  // Perform the request
  CURLcode res;
  try {
    retry(3, std::chrono::milliseconds(500), [&]() {
      auto start = std::chrono::high_resolution_clock::now();

      res = curl_easy_perform(curl);

      auto end = std::chrono::high_resolution_clock::now();
      auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

      if (res != CURLE_OK) {
        std::string err{curl_easy_strerror(res)};
        LOG(WARNING) << "[CLAC] curl request failed to decrypt the key: " << curl_easy_strerror(res);
        throw std::runtime_error(err);
      }
    });
  } catch (const std::exception& e) {
    curl_easy_cleanup(curl);
    VELOX_USER_FAIL("[CLAC] curl request failed to decrypt the key: {}", e.what());
  }

  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);

  curl_easy_cleanup(curl);

  if (httpCode != 200) {
    readBuffer.erase(std::remove(readBuffer.begin(), readBuffer.end(), '\n'), readBuffer.end());
    throw CryptoException("[CLAC] kms request failed with http status code ", httpCode, ": ",  readBuffer);
  }

  return fromJson(readBuffer);
}

const std::string EncryptedKeyVersionEEK = "EEK";

// takes url of format kms://http@localhost:18028/kms
void RangerKMSClient::parseKmsUri(const std::string& kmsUri) {
  static const std::string schema = "kms://";
  kmsUrl_ = kmsUri;
  if (kmsUrl_.compare(0, schema.length(), schema) == 0) {
    kmsUrl_ = kmsUrl_.substr(schema.length());
  }

  static const std::string proto = "http@";
  if (kmsUrl_.compare(0, proto.length(), proto) == 0) {
    kmsUrl_ = "http://" + kmsUrl_.substr(proto.length());
  }
  if (kmsUrl_.back() != '/') {
    kmsUrl_ += '/';
  }
  kmsUrl_ += "v1/";
}

KeyVersion RangerKMSClient::decryptEncryptedKey(std::shared_ptr<EncryptedKeyVersion>& encryptedKeyVersion, const std::string& doAs) {
  // TODO hadoop-common: KMSClientProvider line 798
  VELOX_USER_CHECK(!encryptedKeyVersion->encryptionKeyVersionName.empty(), "[CLAC] encryptionKeyVersionName empty");
  VELOX_USER_CHECK(!encryptedKeyVersion->encryptedKeyIv.empty(), "[CLAC] encryptedKeyIv empty");
  VELOX_USER_CHECK(encryptedKeyVersion->encryptedKeyVersion->versionName == EncryptedKeyVersionEEK,
                   "[CLAC] encryptedKey version name must be '" + EncryptedKeyVersionEEK + "', is '" + encryptedKeyVersion->encryptedKeyVersion->versionName + "'");
  VELOX_USER_CHECK(encryptedKeyVersion->encryptedKeyVersion, "[CLAC] encryptedKeyVersion is null");
  return decrypt(encryptedKeyVersion, doAs);
}

const std::string RangerKMSClient::ARCHIVE_KEY_PREFIX_ = "archive2328d78a045_";

inline bool RangerKMSClient::isLegalHoldKey(std::string& keyName) {
  return keyName.compare(0, ARCHIVE_KEY_PREFIX_.size(), ARCHIVE_KEY_PREFIX_) == 0 || PapyrusHelper::getInstance().isExpiredPapyrusLegalHoldKey(keyName);
}

std::string RangerKMSClient::getKey(const std::string& keyMetadata, bool legalRead, const std::string& doAs) {
  // TODO RangerKMSClient.java#215 process legalRead
  KeyMetadata unAssembledKeyMetadata = KeyMetadataAssembler::unAssembly(keyMetadata);
  std::string versionName = unAssembledKeyMetadata.name + "@" + std::to_string(unAssembledKeyMetadata.version);
  if (!legalRead && isLegalHoldKey(unAssembledKeyMetadata.name)) {
    throw LegalHoldKeyAccessDeniedException("[CLAC] not legal read but accessing legal hold key: " + unAssembledKeyMetadata.name);
  }
  std::shared_ptr<KeyVersion> keyVersion = std::make_shared<KeyVersion>("", EncryptedKeyVersionEEK, unAssembledKeyMetadata.eek);
  std::shared_ptr<EncryptedKeyVersion> encryptedKeyVersion = std::make_shared<EncryptedKeyVersion>(unAssembledKeyMetadata.name, versionName, unAssembledKeyMetadata.iv, keyVersion);

  CacheableEncryptedKeyVersion cacheKey{doAs, encryptedKeyVersion};

  std::optional<std::string> decryptedKeyOpt = cache_.get(cacheKey);
  if (decryptedKeyOpt) {
    return decryptedKeyOpt.value();
  }
  std::optional<std::string> exceptionOpt = exceptionCache_.get(cacheKey);
  if (exceptionOpt) {
    throw CryptoException(exceptionOpt.value());
  }

  std::string decryptedKey{""};
  try {
    decryptedKey = getDecryptedKey(encryptedKeyVersion, doAs);
  } catch (const CryptoException& e) {
    std::string error = e.what();
    if (error.find("http status code 403") != std::string::npos
        || error.find("http status code 404") != std::string::npos
        || error.find("no keyversion exists for key ") != std::string::npos
        || error.find(" not found") != std::string::npos) {
      exceptionCache_.set(cacheKey, error, 300);
    }
    throw;
  }

  cache_.set(cacheKey, decryptedKey, 300); // 5 minutes
  return decryptedKey;
}

std::string RangerKMSClient::getKey(const std::string& keyMetadata, const std::string& doAs) {
  return getKey(keyMetadata, false, doAs);
}

std::string RangerKMSClient::getDecryptedKey(std::shared_ptr<EncryptedKeyVersion>& encryptedKeyVersion, const std::string& doAs) {
  return decryptEncryptedKey(encryptedKeyVersion, doAs).material;
}

}
