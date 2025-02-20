#include "CLACKey.h"
#include <sstream>
#include <regex>
#include <ctime>
#include <iomanip>
#include <stdexcept>
#include <algorithm>
#include <optional>
#include "folly/String.h"

namespace facebook::velox::parquet {

/** CLAC key format: tag, v1_tags or v2_tags_date. Example: v2_tag1-tag2_20221101 */
CLACKey CLACKey::parseKey(const std::string& keyName) {
  if (isBlank(keyName)) {
    throw std::invalid_argument("KeyName is empty");
  }

  std::string version = parseVersion(keyName);
  std::vector<std::string> tags = mapKeyNameToTag(keyName, version);
  std::optional<std::chrono::system_clock::time_point> keyDate = getTimeStampFrom(keyName, version);

  return CLACKey(version, tags, keyDate);
}

bool CLACKey::isBlank(const std::string& str) {
  return str.empty() || std::all_of(str.begin(), str.end(), isspace);
}

std::vector<std::string> CLACKey::mapKeyNameToTag(const std::string& keyName, const std::string& version) {
  if (keyName.empty()) {
    return {};
  }
  if (version.empty()) {
    return {keyName};
  }

  size_t firstSep = keyName.find('_');
  if (version == VERSION1) {
    return splitTags(keyName.substr(firstSep + 1));
  } else if (version == VERSION2) {
    size_t lastSep = keyName.rfind('_');
    if ((firstSep + 1) < keyName.length() && (firstSep + 1) <= lastSep) {
      return splitTags(keyName.substr(firstSep + 1, lastSep - firstSep - 1));
    }
  } else if (!isBlank(version)) {
    return {keyName.substr(firstSep + 1)};
  }

  return {keyName};
}

std::string CLACKey::parseVersion(const std::string& keyName) {
  size_t firstSep = keyName.find('_');
  if (firstSep == std::string::npos) {
    return "";
  }
  std::string version = keyName.substr(0, firstSep);
  std::regex versionPattern("v\\d+");
  if (std::regex_match(version, versionPattern)) {
    return version;
  }
  return "";
}

std::vector<std::string> CLACKey::splitTags(const std::string& tags) {
  if (isBlank(tags)) {
    throw std::invalid_argument("Tag can't be empty");
  }
  std::vector<std::string> result;
  folly::split('-', tags, result, true);
  return result;
}

/**
   * Return timestamp from v2 key name. For v2 month keys like v2_tag_202206 returns first day of
   * month. For v1 returns today. Because v1 is fallback keys if there is issue with parsing
   * partition date. For non v1,v2 keys returns null.
 */
std::optional<std::chrono::system_clock::time_point> CLACKey::getTimeStampFrom(const std::string& keyName, const std::string& version) {
  if (isBlank(keyName)) {
    throw std::invalid_argument("KeyName is empty");
  }
  if (version == VERSION1) {
    return std::chrono::system_clock::now();
  }
  if (version == VERSION2) {
    size_t lastIndex = keyName.rfind('_');
    std::string dateStr = keyName.substr(lastIndex + 1);
    std::tm tm = {};
    std::istringstream ss(dateStr);
    if (dateStr.length() == 6) {
      ss >> std::get_time(&tm, "%Y%m");
      tm.tm_mday = 1;
    } else if (dateStr.length() == 8) {
      ss >> std::get_time(&tm, "%Y%m%d");
    } else {
      throw std::invalid_argument("Wrong key name format keyname=" + keyName);
    }
    if (ss.fail()) {
      throw std::invalid_argument("Failed to parse datetime from key: " + keyName);
    }
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
  }
  return std::nullopt;
}

}
