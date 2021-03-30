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
  static Status Create(const std::string& fname, WritableFile** p_file, size_t init_size, size_t size_addition) {
    void *pmemaddr;
	size_t mapped_len;
	int is_pmem;
	/* create a pmem file and memory map it */
	if ((pmemaddr = pmem_map_file(fname.c_str(), init_size, PMEM_FILE_CREATE|PMEM_FILE_EXCL,
				0666, &mapped_len, &is_pmem)) == NULL) {
		perror("pmem_map_file");
		exit(1);
	}
    *p_file=new PMWritableFile(pmemaddr,init_size,size_addition,fname);
    return Status::OK();
    }

Status Append(const Slice& slice) override {
    size_t len = slice.size();
    // the least file size to write the new slice
    size_t least_file_size = sizeof(size_t) + data_length + len;
    // left space is not enough, need expand
    while (least_file_size > file_size) {
      //remmap the file with larger file_size
      size_t mapped_len;
	    int is_pmem;
        //remmap the file with larger file size
        pmem_unmap(map_base,file_size);
        map_base= pmem_map_file(fname.c_str(),file_size+size_addition,0,0,&mapped_len,&is_pmem);
        if(map_base==NULL){
            perror("pmem_remap_file");
		    exit(1);
        }
        file_size+=size_addition;
    }
    pmem_memcpy_nodrain(map_base+sizeof(size_t)+data_length,slice.data(),len);
    data_length += len;
    return Status::OK();
  }

  Status Truncate(uint64_t size) override {
    while(size > file_size){
	    int is_pmem;
        //remmap the file with larger file size
        pmem_unmap(map_base,file_size);
        map_base= pmem_map_file(fname.c_str(),file_size+size_addition,0,0,&map_length,&is_pmem);
        if(map_base==NULL){
            perror("pmem_remap_file");
		    exit(1);
        }
        file_size+=size_addition;
    }
    data_length = size;
    return Status::OK();
  }

  Status Close() override {
    pmem_unmap(map_base,map_length);
    return Status::OK();
  }

  Status Flush() override {
    // DCPMM need no flush
    return Status::OK();
  }

  Status Sync() override {
    // sync metadata filesize
    pmem_memset_persist(map_base,file_size,sizeof(size_t));
    return Status::OK();
  }

  uint64_t GetFileSize() override {
    // it is the data length, not the physical space size
    return data_length;
  }

 private:
    PMWritableFile(void* base,size_t init_size, size_t _size_addition,const std::string& fname) : file_size(init_size), size_addition(_size_addition), map_base(base), map_length(init_size), fname(fname){
    // reset the data length in offset 0 
    pmem_memset_persist(map_base,0,sizeof(size_t));
  }

  private:
  size_t data_length;       // the writen data length
  size_t file_size;         // the length of the whole file
  size_t size_addition;     // expand size_addition bytes
                            // every time left space is not enough
  void* map_base;           // mmap()-ed area
  size_t map_length;        // mmap()-ed length
  std::string fname;
};

class PMSequentialFile : public SequentialFile {
 public:
 private:
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
  Status s= PMWritableFile::Create(fname,&file,init_size_,size_additon_);
  if(s.ok()){
      result->reset(file);
  }
  return s;
}


Status PMEnv::DeleteFile(const std::string& fname) {
    return EnvWrapper::DeleteFile(fname);
}


Status PMEnv::NewSequentialFile(const std::string& fname,
                                std::unique_ptr<SequentialFile>* result,
                                const EnvOptions& options) {
  return Status::OK();
}

}  // namespace rocksdb
