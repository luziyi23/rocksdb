#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb {

class PMEnv : public EnvWrapper {
 public:
  PMEnv(Env* base_env) : EnvWrapper(base_env) {}

 private:
  // we should use DCPMM-awared filesystem for WAL file
  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& env_options) override;
  //   Status DeleteFile(const std::string& fname)override;
  // The WAL file may have extra format, so should be handled before reading
  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;
  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override;

  Status GetFileSize(const std::string& f, uint64_t* s) override;

      private : size_t init_size_ = 128 << 20;
  size_t size_additon_ = 32 << 20;
};

}  // namespace rocksdb