#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"


namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

//read_directory: Given the inode id of a directory, read its contents parse into entries.

//rm_from_directory: Given the contents of a directory, remove an entry by the given filename from the contents.
auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
    auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

        // TODO: Implement this function.
        //       Append the new directory entry to `src`.
        src += "/" + filename + ":" +inode_id_to_string(id);
        return src;
    }

// {Your code here}
    void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

        // TODO: Implement this function.
        // UNIMPLEMENTED();
        std::istringstream ss(src);
        std::string token;
        while (std::getline(ss, token, '/')) {
            size_t pos = token.find(':');
            if (pos != std::string::npos) {
                std::string name = token.substr(0, pos);
                std::string id = token.substr(pos + 1);
                DirectoryEntry directoryEntry;
                directoryEntry.id = string_to_inode_id(id);
                directoryEntry.name = name;
                list.emplace_back(directoryEntry);
            }
        }

    }

/**
 * Remove an entry from the directory.
 *
 * @param src: the string to remove from
 * @param filename: the filename to remove
 */
// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");
  std::list<DirectoryEntry> list;
  parse_directory(src,list);
  // 查找并删除要删除的`filename`
        for (auto it = list.begin(); it != list.end(); ++it) {
            if (it->name == filename) {
                list.erase(it);
                break;
            }
        }
        res = dir_list_to_string(list);
  return res;
}
/**
 * Read the directory information.
 * We assume that the directory information is stored as a
 * "name0:inode0/name1:inode1/ ..." string in the file blocks.
 *
 * @param fs: the pointer to the file system
 * @param inode: the inode number of the directory
 * @param list: the list to store the read content
 *
 * ## Warn: we don't check whether the inode is a directory or not
 * We assume the upper layer should handle this
 */
/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  // TODO: Implement this function.
        // 获取目录的inode
        auto result = fs->read_file(id);
        // 检查读取是否成功
        if (result.is_err()) {
            return result.unwrap_error(); // 处理读取错误
        }
        // 解析读取到的内容，将每个目录条目添加到list中
        std::vector<u8> content = result.unwrap();
        std::string content_str(content.begin(), content.end());
        parse_directory(content_str,list);
        return KNullOk;
}
    /**
     * Lookup the directory
     */
// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
    std::list<DirectoryEntry> list;

    read_directory(this, id, list);
        for(auto file : list){
            if(std::string(name) == file.name){
                return ChfsResult<inode_id_t>(file.id);
            }
        }
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type,std::vector<std::shared_ptr<BlockOperation>> *ops)
    -> ChfsResult<inode_id_t> {
        // TODO:
        // 1. Check if `name` already exists in the parent.
        //    If already exist, return ErrorType::AlreadyExist.
        std::list<DirectoryEntry> list;
        read_directory(this, id, list);
        for(auto file : list){
            if(std::string(name) == file.name){
                return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
            }
        }
        // 2. Create the new inode.
        inode_id_t inode_id;
        auto result_alloc_node = alloc_inode(type,&inode_id,ops);;
        if(result_alloc_node.is_err()){
            std::cerr<<typeid(result_alloc_node.unwrap_error()).name()<<"  nonononono\n";
            return result_alloc_node.unwrap_error();
        }
        inode_id = result_alloc_node.unwrap();
        // 3. Append the new entry to the parent directory.
        std::string dir = dir_list_to_string(list);
        std::string tmp = append_to_directory(dir, std::string(name), inode_id);
        auto buffer = std::vector<u8>(tmp.begin(),tmp.end());
        std::cout<<"writing "<<name<<std::endl;
        auto result_parent = write_file(id, buffer,ops);
        if(result_parent.is_err()){
            return result_parent.unwrap_error();
        }
        return ChfsResult<inode_id_t>(inode_id);

        // return ChfsResult<inode_id_t>(static_cast<inode_id_t>(0));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name,std::vector<std::shared_ptr<BlockOperation>> *ops)
    -> ChfsNullResult {

  // TODO:
    // 1. Remove the file, you can use the function `remove_file`
    auto inode_id = lookup(parent,name).unwrap();
    remove_file(inode_id,ops);

    // 2. Remove the entry from the directory.
    std::list<DirectoryEntry> list;
    read_directory(this,parent,list);
    std::string new_list = rm_from_directory(dir_list_to_string(list),name);
    auto content = std::vector<u8>(new_list.begin(),new_list.end());
    write_file(parent, content,ops);
    return KNullOk;
}

} // namespace chfs
