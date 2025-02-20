#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include "velox/dwio/parquet/crypto/AesEncryption.h"
#include "velox/dwio/parquet/crypto/ColumnPath.h"
#include "velox/dwio/parquet/crypto/FileDecryptionProperties.h"
#include "velox/dwio/parquet/thrift/ParquetThriftTypes.h"

namespace facebook::velox::parquet {

class ColumnDecryptionSetup;
using ColumnPathToDecryptiorSetupMap =
    std::map<std::string, std::shared_ptr<ColumnDecryptionSetup>>;

class Decryptor {
 public:
  Decryptor(std::shared_ptr<AesDecryptor> decryptor, std::string  key,
            std::string  fileAad);

  const std::string& fileAad() const { return fileAad_; }
//  void updateAad(const std::string& aad) { aad_ = aad; }

  [[nodiscard]] int plaintextLength(int ciphertextLen) const;
  [[nodiscard]] int ciphertextLength(int plaintextLen) const;
  int decrypt(const uint8_t* ciphertext, int ciphertextLen,
              uint8_t* plaintext, int plaintextLen, std::string_view aad);

  std::shared_ptr<AesDecryptor> getAesDecryptor() {return aesDecryptor_;}

 private:
  std::shared_ptr<AesDecryptor> aesDecryptor_;
  std::string key_;
  std::string fileAad_;
};

class ColumnDecryptionSetup {
 public:
  explicit ColumnDecryptionSetup(ColumnPath& columnPath, bool encrypted, bool keyAvailable,
                                         std::shared_ptr<Decryptor> dataDecryptor,
                                         std::shared_ptr<Decryptor> metadataDecryptor,
                                         int columnOrdinal, bool legalHoldMasking,
                                         std::string_view savedException) :
        columnPath_(columnPath), encrypted_(encrypted), keyAvailable_(keyAvailable),
        dataDecryptor_(std::move(dataDecryptor)), metadataDecryptor_(std::move(metadataDecryptor)),
        columnOrdinal_(columnOrdinal), legalHoldMasking_(legalHoldMasking),
        savedException_(savedException) {}

  ColumnPath getColumnPath() {return columnPath_;}
  bool isEncrypted() {return encrypted_;}
  std::shared_ptr<Decryptor> getDataDecryptor () {return dataDecryptor_;}
  std::shared_ptr<Decryptor> getMetadataDecryptor () {return metadataDecryptor_;}
  int getColumnOrdinal() {return columnOrdinal_;}
  bool isLegalHoldMasking() {return legalHoldMasking_;}
  bool isKeyAvailable() {return keyAvailable_;}
  std::string savedException() {return savedException_;}

 private:
  ColumnPath columnPath_;
  bool encrypted_;
  bool keyAvailable_;
  std::shared_ptr<Decryptor> dataDecryptor_;
  std::shared_ptr<Decryptor> metadataDecryptor_;
  int columnOrdinal_;
  bool legalHoldMasking_;
  std::string savedException_;
};

class FileDecryptor {
 public:
  FileDecryptor(FileDecryptionProperties* properties,
                        std::string  fileAad,
                        ParquetCipher::type algorithm,
                        std::string user);

  ParquetCipher::type algorithm() { return algorithm_; }

  FileDecryptionProperties* properties() { return properties_; }

  std::string& user() { return user_; }

  std::shared_ptr<ColumnDecryptionSetup> setColumnCryptoMetadata(
      ColumnPath& columnPath,
      bool encrypted,
      std::string& keyMetadata,
      int columnOrdinal);

  std::shared_ptr<ColumnDecryptionSetup> getColumnCryptoMetadata(std::string& columnPath) {
      auto it = columnPathToDecryptiorSetupMap_.find(columnPath);
      if (it == columnPathToDecryptiorSetupMap_.end()) {
        return nullptr;
      }
      return it->second;
  }

  static std::string handleAadPrefix(FileDecryptionProperties* fileDecryptionProperties, thrift::EncryptionAlgorithm& encryptionAlgorithm);
  static ParquetCipher::type getEncryptionAlgorithm(thrift::EncryptionAlgorithm& encryptionAlgorithm);

 private:
  std::shared_ptr<Decryptor> getColumnMetaDecryptor(
      const std::string& column_key);
  std::shared_ptr<Decryptor> getColumnDataDecryptor(
      const std::string& column_key);
  //  std::shared_ptr<Decryptor> GetFooterDecryptor(const std::string& aad, bool metadata);
  std::shared_ptr<Decryptor> getColumnDecryptor(
      const std::string& columnKey,
      bool metadata = false);

  FileDecryptionProperties* properties_;
  std::string fileAad_;
  ParquetCipher::type algorithm_;
  // Mutex to guard access to allDecryptors_
  mutable std::mutex mutex_;
  // A weak reference to all decryptors that need to be wiped out when decryption is
  // finished, guarded by mutex_ for thread safety TODO implement this
  std::vector<std::weak_ptr<AesDecryptor>> allDecryptors_;
  ColumnPathToDecryptiorSetupMap columnPathToDecryptiorSetupMap_;
  bool legalHoldMasking_; //TODO initialize
  std::string user_;
};

}
