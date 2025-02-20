#include "PapyrusHelper.h"
#include "CLACKey.h"
#include <iostream>
#include <algorithm>

namespace facebook::velox::parquet {

bool PapyrusHelper::isExpiredPapyrusLegalHoldKey(const std::string& keyName, const std::chrono::system_clock::time_point& now) const {
  try {
    if (!containsLegalHoldPrefix(keyName)) {
      return false;
    }

    CLACKey key = CLACKey::parseKey(keyName);
    if (!key.getVersion().empty() && key.getKeyInstant()) {
      const std::vector<std::string>& tags = key.getTags();
      std::vector<std::string> googleTags;
      for (const auto & tag : tags) {
        if (isGoogleTag(tag)) {
          googleTags.push_back(tag);
        }
      }
      auto minDuration = std::min_element(
          googleTags.begin(), googleTags.end(),
          [this](const std::string& tag1, const std::string& tag2) {
            return LEGAL_HOLD_PERIODS_BY_TAG.at(tag1) < LEGAL_HOLD_PERIODS_BY_TAG.at(tag2);
          });

      if (minDuration != googleTags.end()) {
        auto holdDuration = LEGAL_HOLD_PERIODS_BY_TAG.at(*minDuration);
        auto holdStartDay = *key.getKeyInstant() + holdDuration;
        return holdStartDay < now;
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "Failed to verify legal hold for key: " << keyName << " - " << e.what() << std::endl;
  }
  return false;
}

}
