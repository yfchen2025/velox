#pragma once
#include <string>
#include <unordered_map>
#include <chrono>

namespace facebook::velox::parquet {

class PapyrusHelper {
 public:
  static PapyrusHelper& getInstance() {
    static PapyrusHelper instance;
    return instance;
  }

  bool containsLegalHoldPrefix(const std::string& keyName) const {
    return !keyName.empty() && keyName.find(PAPYRUS_LEGAL_HOLD_KEY_PREFIX) != std::string::npos;
  }

  bool isExpiredPapyrusLegalHoldKey(const std::string& keyName) const {
    return isExpiredPapyrusLegalHoldKey(keyName, std::chrono::system_clock::now());
  }

  bool isExpiredPapyrusLegalHoldKey(const std::string& keyName, const std::chrono::system_clock::time_point& now) const;

 private:
  PapyrusHelper() {
    LEGAL_HOLD_PERIODS_BY_TAG[GOOGLE_ADDRESS_TAG] = std::chrono::hours(24 * 30);
    LEGAL_HOLD_PERIODS_BY_TAG[GOOGLE_LATLNG_TAG] = std::chrono::hours(24 * 730);
  }

  const std::string GOOGLE_ADDRESS_TAG = "google_address";
  const std::string GOOGLE_LATLNG_TAG = "google_latlng";
  const std::string PAPYRUS_LEGAL_HOLD_KEY_PREFIX = "google";
  std::unordered_map<std::string, std::chrono::system_clock::duration> LEGAL_HOLD_PERIODS_BY_TAG;

  bool isGoogleTag(const std::string& tag) const {
    return GOOGLE_ADDRESS_TAG == tag || GOOGLE_LATLNG_TAG == tag;
  }
};

}
