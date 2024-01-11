#include "distributed/dataserver.h"
#include "common/util.h"
namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 0, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 0, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                     usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
  current_version = 0;
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
 std::vector<u8> buffer(DiskBlockSize);
 auto num_per_block = DiskBlockSize / sizeof(version_t);
 auto version_id = block_id / num_per_block;
 auto version_offset = block_id % num_per_block;
 auto version_num = version_offset*sizeof(version_t)/sizeof(u8);
 auto result = block_allocator_->bm->read_block(version_id,buffer.data());
 if(result.is_err()){
     return {};
 }
 auto get_version = *(version_t *)(buffer.data()+version_num);
 auto result_read = block_allocator_->bm->read_block(block_id,buffer.data());
 if(get_version!=version){
     std::cerr<<"get version wrong!";
     return {};
 }
 if(result_read.is_err()){
     return {};
 }

 std::vector<u8> content(len);
 std::move(buffer.begin()+offset,buffer.begin()+offset+len,content.begin());
 return content;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
//  // TODO: Implement this function.
        auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(),
                                                             offset, buffer.size());
        if (res.is_err()) {
            return false;
        }
        return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  std::vector<u8> buffer(DiskBlockSize);
  // allocate area
  auto num_per_block = DiskBlockSize/sizeof(version_t);
  auto result = block_allocator_->allocate(nullptr);
  auto block_id = result.unwrap();
  auto version_id = block_id/num_per_block;
  auto version_offset = block_id%num_per_block;
  auto version_num = version_offset*sizeof(version_t)/sizeof(u8);
  auto result_block = block_allocator_->bm->read_block(version_id,buffer.data());
  if(result_block.is_err()){
      return {};
  }
  auto version = (version_t *)(buffer.data()+version_num);
  *version += 1;
  auto result_write_back = block_allocator_->bm->write_block(version_id,buffer.data());
  if(result_write_back.is_err())return {};
  return {block_id,*version};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
        auto num_per_block = DiskBlockSize / sizeof(version_t);
        auto res = block_allocator_->deallocate(block_id);
        if (res.is_err()) {
            return false;
        }
        std::vector<u8> buffer(DiskBlockSize);
        auto version_block_id = block_id / num_per_block;
        auto version_block_offset = block_id % num_per_block;
        auto res1 = block_allocator_->bm->read_block(version_block_id, buffer.data());
        if (res1.is_err()) return false;
        auto *version =
                (version_t *)(buffer.data() +
                              version_block_offset * (sizeof(version_t) / sizeof(u8)));
        *version += 1;
        auto res2 =
                block_allocator_->bm->write_block(version_block_id, buffer.data());
        if (res2.is_err()) return false;
        return true;

}
} // namespace chfs