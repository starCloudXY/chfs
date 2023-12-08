#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>
int read_counter = 0;
namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}
void MetadataServer::meta_lock_all() {
    for (auto &item : meta_mtx) {
        item.lock();
    }
}
void MetadataServer::meta_unlock_all() {
    for (int i = meta_mtx_num - 1; i >= 0; i--) {
        meta_mtx[i].unlock();
    }
}
// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.

  meta_lock_all();
  if (is_checkpoint_enabled_) {
      commit_log->checkpoint();
  }
  ChfsResult<inode_id_t> res(0);
        std::vector<std::shared_ptr<BlockOperation>> ops;
        if (is_log_enabled_) {
            res = this->operation_->mk_helper(parent, name.data(), (InodeType)(type),
                                              &ops);
        } else {
            res = this->operation_->mk_helper(parent, name.data(), (InodeType(type)),
                                              nullptr);
        }

  meta_unlock_all();
  if (is_log_enabled_) {
      auto local_txn_id = ++txn_id;
      commit_log->append_log(local_txn_id, ops);
      commit_log->commit_log(local_txn_id);
  }
  if(res.is_ok()){
      std::cout<<"make node ok\n";
      return res.unwrap();
  }
  else{
      std::cerr<<"cannot make a node";
      return 0;
  }
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {

    meta_lock_all();
    if(!is_checkpoint_enabled_){
        commit_log->checkpoint();
    }
  // TODO: Implement this function.
        std::cout<<"begin unlink node\n";
        std::vector<std::shared_ptr<BlockOperation>> ops;
//        auto *param = is_log_enabled_ ? &ops : nullptr;
  auto result = operation_->unlink(parent,name.data());
        std::cout<<"finish unlink node\n";
        meta_unlock_all();
        if (is_log_enabled_) {
            auto local_txn_id = ++txn_id;
            commit_log->append_log(local_txn_id, ops);
            commit_log->commit_log(local_txn_id);
        }
        if (result.is_err()){
      std::cerr<<"fail to delete the block!!!";
      return false;
  }
  else return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
    //parents?
  // TODO: Implement this function.
  std::shared_lock<std::shared_mutex> sharedLock(meta_mtx[meta_lock_num(parent)]);
  auto result = this->operation_->lookup(parent,name.data());
  if(result.is_err()){
      std::cerr<<("no block found!!!!");
      return 0;
  }
  return result.unwrap();
}

// {Your code here}

auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  //step1:get block
    std::vector<u8> file;
    {
        std::shared_lock<std::shared_mutex> sharedLock(meta_mtx[meta_lock_num(id)]);
        auto result = operation_->read_file(id);
        file = result.unwrap();
    }
  std::vector<BlockInfo> info;
  auto size = sizeof (BlockInfo);
  for(unsigned long i = 0;i< file.size();i += size){
      auto tuple = *(BlockInfo *)(file.data() + i);
      auto [block_id, mac_id, version] = tuple;
      info.emplace_back(block_id, mac_id, version);
  }
  return info;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
    auto mac_id = generator.rand(1, this->num_data_servers);
    std::unique_lock<std::shared_mutex> uniqueLock(data_mtx[data_lock_num(mac_id)]);
    auto client = clients_[mac_id];
    auto result = client->call("alloc_block");
    uniqueLock.unlock();
    if (result.is_err()) return {};
    auto tuple_pair =
            result.unwrap()->as<std::pair<block_id_t, version_t>>();
    std::lock_guard<std::shared_mutex> lockGuard(meta_mtx[meta_lock_num(id)]);
    auto result_read = operation_->read_file(id);
    auto data = result_read.unwrap();
    std::vector<u8> content(sizeof(BlockInfo));
    auto p = (BlockInfo*)content.data();
    *p = std::make_tuple(tuple_pair.first,mac_id,tuple_pair.second);
    data.insert(data.end(), content.begin(), content.end());
    auto result_write_back = operation_->write_file(id,data, nullptr);
    if(result_write_back.is_err())return {};
    return {tuple_pair.first, mac_id, tuple_pair.second};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {

  // TODO: Implement this function.
  // TODO： need to delete the record on metadata server,but wait... where is the record?
        {
            std::lock_guard<std::shared_mutex> lockGuard(
                    data_mtx[data_lock_num(machine_id)]);
            auto client = clients_[machine_id];
            auto result = client->call("free_block", block_id);
            if (result.is_err()) {
                std::cerr << "no block to free!";
                return false;
            }
        }
            std::lock_guard<std::shared_mutex> lockGuard(
                    meta_mtx[data_lock_num(id)]);
            auto result_i = operation_->read_file(id);
            auto file = result_i.unwrap();
            auto pTuple = reinterpret_cast<BlockInfo *>(file.data());
            auto size = file.size();
            auto block_size = sizeof(BlockInfo);
            for(unsigned long i = 0;i < size;i += block_size){
                pTuple =(BlockInfo *)(file.data() + i);;
                if(std::get<0>(*pTuple)==block_id&&
                   std::get<1>(*pTuple)==machine_id){
                    file.erase(file.begin()+(long)i,
                               file.begin()+(long)i+(long)block_size);
                    this->operation_->write_file(id,file, nullptr);
                    return true;
                }
            }
            return false;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  std::list<DirectoryEntry>list;
  std::vector<std::pair<std::string, inode_id_t>> content;
        {
            std::lock_guard<std::shared_mutex> lockGuard(
                    data_mtx[data_lock_num(node)]);
            auto result = read_directory(operation_.get(),node,list);
            if(result.is_err()){
                std::cerr<<"Fail to read inode !!!!!!!!!";
            }
        }
  for (const auto &item : list) {
      content.emplace_back(item.name, item.id);
  }
  return content;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  std::shared_lock<std::shared_mutex> lockGuard(meta_mtx[data_lock_num(id)]);
  auto result = operation_->get_type_attr(id);
  if(result.is_err())return {};
  auto type = result.unwrap().first;
  auto file_addr = result.unwrap().second;
  if(type==InodeType::FILE){
      auto vec = get_block_map(id);
      file_addr.size = vec.size()*DiskBlockSize;
  }
  return {file_addr.size,file_addr.atime,file_addr.mtime, file_addr.ctime,(u8)type};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));
  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs