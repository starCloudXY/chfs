#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto result = metadata_server_->call("mknode", (u8)type, parent, name);
  if (result.is_err())
      return {result.unwrap_error()};
  if (result.unwrap()->as<inode_id_t>() == 0)
      return {
      ErrorType::INVALID};
  return {
      result.unwrap()->as<inode_id_t>()
  };
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  auto result = metadata_server_->call("unlink", parent, name);
  if (result.is_err()) return result.unwrap_error();
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto result = metadata_server_->call("lookup", parent, name);
  if (result.is_err()) return result.unwrap_error();
  return {result.unwrap()->as<inode_id_t>()};
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("readdir", id);
  if (res.is_err()) return res.unwrap_error();
  return {res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>()};
}


// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
        auto res = metadata_server_->call("get_type_attr", id);
        if (res.is_err()) return res.unwrap_error();
        std::pair<InodeType, FileAttr> ret;
        // size, atime, mtime, ctime
        auto [size, atime, mtime, ctime, type] =
                res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
        ret.first = static_cast<InodeType>(type);
        ret.second.size = size;
        ret.second.atime = atime;
        ret.second.mtime = mtime;
        ret.second.ctime = ctime;
        return {ret};
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("get_block_map", id);
  if (res.is_err()) return res.unwrap_error();
  auto block_info = res.unwrap()->as<std::vector<BlockInfo>>();
  auto file_size = block_info.size() * DiskBlockSize;
  std::vector<u8> content;
  auto block_info_index = offset / DiskBlockSize;
  auto block_info_offset = offset % DiskBlockSize;
  auto read_size = file_size;
  while (size > 0) {
      auto left_size_in_block = DiskBlockSize - block_info_offset;
      read_size = size > left_size_in_block ? left_size_in_block : size;
      auto [blockId, macId, version] = block_info.at(block_info_index);
      auto res1 = data_servers_[macId]->call(
                    "read_data", blockId, block_info_offset, read_size, version);
      if (res1.is_err()) return res1.unwrap_error();
      auto data = res1.unwrap()->as<std::vector<u8>>();
      content.insert(content.end(), data.begin(), data.end());

      size -= read_size;
      block_info_index++;
      block_info_offset = 0;
  }
  return {content};
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
        auto res = metadata_server_->call("get_block_map", id);
        if (res.is_err()) return res.unwrap_error();
        auto block_info = res.unwrap()->as<std::vector<BlockInfo>>();
        auto block_info_index = offset / DiskBlockSize;
        auto block_info_offset = offset % DiskBlockSize;
        auto write_len = data.size();
        size_t write_start = 0;
        while (write_len > 0) {
            auto write_size = std::min((size_t)(DiskBlockSize-block_info_offset),size_t(write_len));
            while (block_info.size() <= block_info_index) {
                auto res_alloc = metadata_server_->call("alloc_block", id);
                if (res_alloc.is_err()) return res_alloc.unwrap_error();
                block_info.emplace_back(res_alloc.unwrap()->as<BlockInfo>());
            }
            auto [blockId, macId, version] = block_info[block_info_index];
            std::vector<u8> buffer;
            buffer.insert(buffer.end(),data.data()+write_start,data.data()+write_start+write_size);
            auto res1 = data_servers_[macId]->call(
                    "write_data", blockId, block_info_offset,buffer);
            //    auto res2 = data_servers_[macId]->call()
            if (res1.is_err()) return res1.unwrap_error();
            write_len -= write_size;
            write_start += write_size;
            block_info_index++;
            block_info_offset = 0;
        }
        return KNullOk;
    }
// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  auto res = metadata_server_->call("free_block", id, block_id, mac_id);
  return KNullOk;
}

} // namespace chfs