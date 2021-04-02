#include "pmenv/env_pm.h"

#include <errno.h>
#include <fcntl.h>
#include <libpmem.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "rocksdb/env.h"

namespace rocksdb {

namespace {

class PMWritableFile : public WritableFile {
 public:
  static Status Create(const std::string& fname, WritableFile** p_file,
                       size_t init_size, size_t size_addition) {
    void* pmemaddr;
    size_t mapped_len;
    int is_pmem;
    /* create a pmem file and memory map it */
    if ((pmemaddr = pmem_map_file(fname.c_str(), init_size,
                                  PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666,
                                  &mapped_len, &is_pmem)) == NULL) {
      perror("pmem_map_file");
      exit(1);
    }
    *p_file = new PMWritableFile((uint8_t*)pmemaddr, init_size, size_addition, fname);
    return Status::OK();
  }
  Status Append(const Slice& slice,const DataVerificationInfo& /* verification_info */)override{
      return Append(slice);
  }
  Status Append(const Slice& slice) override {
    size_t len = slice.size();
    // the least file size to write the new slice
    size_t least_file_size = sizeof(size_t) + data_length + len;
    // left space is not enough, need expand
    if (least_file_size > file_size) {
      size_t count = (least_file_size - file_size - 1) / size_addition + 1;
      size_t add_size = count * size_addition;
      // remmap the file with larger file_size
      size_t mapped_len;
      int is_pmem;
      // remmap the file with larger file size
      pmem_unmap(map_base, file_size);
      map_base = (uint8_t*)pmem_map_file(fname_.c_str(), file_size + add_size, PMEM_FILE_CREATE, 0666,&mapped_len, &is_pmem);
      if (map_base == NULL) {
        perror("append: pmem_remap_file");
        exit(1);
      }
      file_size += add_size;
    }
    pmem_memcpy_persist(map_base + sizeof(size_t) + data_length, slice.data(),len);
    // memcpy(map_base + sizeof(size_t) + data_length, slice.data(),len);
    data_length += len;
    return Status::OK();
  }

  Status Truncate(uint64_t size) override {
    if (size > file_size) {
      size_t count = (size - file_size - 1) / size_addition + 1;
      size_t add_size = count * size_addition;
      int is_pmem;
      // remmap the file with larger file size
      pmem_unmap(map_base, file_size);
      map_base = (uint8_t*)pmem_map_file(fname_.c_str(), file_size + add_size, PMEM_FILE_CREATE, 0666,&file_size, &is_pmem);
      if (map_base == NULL) {
        perror("truncate: pmem_remap_file");
        exit(1);
      }
    }
    data_length = size;
    return Status::OK();
  }

  Status Close() override {
    Sync();
    // printf("fname:%s closed,filesize=%ldMB,datasize=%ldMB,real_datasize=%ldMB\n",fname_.c_str(),file_size>>20,data_length>>20,*((size_t*)map_base)>>20);
    pmem_unmap(map_base, file_size);
    return Status::OK();
  }

  Status Flush() override {
    // DCPMM need no flush
    return Status::OK();
  }

  Status Sync() override {
    // sync the cacheline which is not persist
    // if(data_length>persist_length_){
    //     pmem_persist(map_base+sizeof(size_t)+persist_length_,data_length-persist_length_);
    //     persist_length_=data_length;
    // }
    // write file metadata and persist
    // pmdk write 
    pmem_memcpy_persist((void*)map_base, &data_length, sizeof(size_t));
    // force ntstore write
    // __builtin_ia32_movnti64((long long*)map_base, data_length); __builtin_ia32_sfence();
    return Status::OK();
  }

  uint64_t GetFileSize() override {
    // it is the data length, not the physical space size
    return data_length;
  }

 private:
  PMWritableFile(uint8_t* base, size_t init_size, size_t _size_addition,
                 const std::string& fname)
      : data_length(0),persist_length_(0),
      file_size(init_size),
        size_addition(_size_addition),
        map_base(base),
        fname_(fname) {
    // reset the data length in offset 0
    pmem_memset_persist(map_base, 0, sizeof(size_t));
  }

 private:
  size_t data_length;    // the writen data length
  size_t persist_length_;
  size_t file_size;      // the length of the whole file
  size_t size_addition;  // expand size_addition bytes
                         // every time left space is not enough
  uint8_t* map_base;        // mmap()-ed area
  std::string fname_;
};

class PMSequentialFile : public SequentialFile {
 public:
    static Status Open(const std::string& fname, SequentialFile** p_file) {
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      return Status::IOError(std::string("PMopen '").append(fname)
                              .append("' failed: ").append(strerror(errno)));
    }
    // get the file size
    struct stat stat;
    if (fstat(fd, &stat) < 0) {
      close(fd);
      return Status::IOError(std::string("PMfstat '").append(fname).
                              append("' failed: ").append(strerror(errno)));
    }
    close(fd);
    size_t maplen;
    int ispmem;
    // whole file
    void* base = pmem_map_file(fname.c_str(),stat.st_size,PMEM_FILE_CREATE,0666,&maplen,&ispmem);
    // no matter map ok or fail, we can close now
    if (base == NULL) {
      return Status::IOError(std::string("PMmmap file failed: ")
                              .append(strerror(errno)));
    }
    // printf("openfile:%s file_size=%ld",fname.c_str(),stat.st_size>>20);
    *p_file = new PMSequentialFile(base, (size_t)stat.st_size);
    return Status::OK();
  }

Status Read(size_t n, Slice* result, char* /*scratch*/) override {
    assert(seek <= data_length);
    auto len = std::min(data_length - seek, n);
    *result = Slice((char*)data + seek, len);
    seek += len;
    return Status::OK();
  }
  Status Skip(uint64_t n) override {
    assert(seek <= data_length);
    auto len = std::min(data_length - seek, n);
    seek += len;
    return Status::OK();
  }

    ~PMSequentialFile() {
    void* base = (void*)(data - sizeof(size_t));
    pmem_unmap(base,file_size);
  }
 private:
    PMSequentialFile(void* base, size_t _file_size):file_size(_file_size) {
        data_length = *((size_t*)base);
        // printf("data_size=%ldMB\n",data_length>>20);
        data = (uint8_t*)base + sizeof(size_t);
        seek = 0;
    }
private:
  size_t file_size;       // the length of the whole file
  uint8_t* data;          // the base address of data
  size_t data_length;     // the length of data
  size_t seek;            // next offset to read
};

}  // Anonymous namespace
static bool EndsWith(const std::string& str, const std::string& suffix) {
  auto str_len = str.length();
  auto suffix_len = suffix.length();
  if (str_len < suffix_len) {
    return false;
  }
  return memcmp(str.c_str() + str_len - suffix_len, suffix.c_str(),
                suffix_len) == 0;
}

static bool IsWALFile(const std::string& fname) {
  return EndsWith(fname, ".log");
}

Status PMEnv::NewWritableFile(const std::string& fname,
                              std::unique_ptr<WritableFile>* result,
                              const EnvOptions& env_options) {
  // if it is not a log file, then fall back to original logic
  if (!IsWALFile(fname)) {
    return EnvWrapper::NewWritableFile(fname, result, env_options);
  }
  WritableFile* file = nullptr;
  // don not use recycle log
  Status s = PMWritableFile::Create(fname, &file, init_size_, size_additon_);
  if (s.ok()) {
    result->reset(file);
  }
  return s;
}
Status PMEnv::DeleteFile(const std::string& fname) {
  // if it is not a log file, then fall back to original logic
//   printf("deletefile: %s\n",fname.c_str());
    return EnvWrapper::DeleteFile(fname);
}
Status PMEnv::NewSequentialFile(const std::string& fname,
                                std::unique_ptr<SequentialFile>* result,
                                const EnvOptions& env_options) {
    if (!IsWALFile(fname)) {
    return EnvWrapper::NewSequentialFile(fname, result, env_options);
  }
  SequentialFile* file=nullptr;
  Status s = PMSequentialFile::Open(fname,&file);
  if(s.ok()){
      result->reset(file);
  }
  return s;
}

Env* NewPMEnv(Env* base_env) {
  return new PMEnv(base_env ? base_env : rocksdb::Env::Default());
}


}  // namespace rocksdb
