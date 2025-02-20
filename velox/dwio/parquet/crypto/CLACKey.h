#pragma once
#include <string>
#include <vector>
#include <chrono>
#include <optional>

namespace facebook::velox::parquet {

class CLACKey {
 public:
  static constexpr std::string_view VERSION1{"v1"};
  static constexpr std::string_view VERSION2{"v2"};

  CLACKey(const std::string& version, const std::vector<std::string>& tags, const std::optional<std::chrono::system_clock::time_point>& keyInstant)
      : version(version), tags(tags), keyInstant(keyInstant) {}

  std::string getVersion() const {
    return version;
  }

  const std::vector<std::string>& getTags() const {
    return tags;
  }

  std::optional<std::chrono::system_clock::time_point> getKeyInstant() const {
    return keyInstant;
  }

  /** CLAC key format: tag, v1_tags or v2_tags_date. Example: v2_tag1-tag2_20221101 */
  static CLACKey parseKey(const std::string& keyName);

 private:
  std::string version;
  std::vector<std::string> tags;
  std::optional<std::chrono::system_clock::time_point> keyInstant;

  static bool isBlank(const std::string& str);
  static std::string parseVersion(const std::string& keyName);
  static std::vector<std::string> mapKeyNameToTag(const std::string& keyName, const std::string& version);
  static std::vector<std::string> splitTags(const std::string& tags);
  /**
   * Return timestamp from v2 key name. For v2 month keys like v2_tag_202206 returns first day of
   * month. For v1 returns today. Because v1 is fallback keys if there is issue with parsing
   * partition date. For non v1,v2 keys returns null.
   */
  static std::optional<std::chrono::system_clock::time_point> getTimeStampFrom(const std::string& keyName, const std::string& version);
};

}
