#include <ctime>

#include "filesystem/operations.h"

namespace chfs {

// {Your code here}
    auto FileOperation::alloc_inode(
            InodeType type, std::vector<std::shared_ptr<BlockOperation>> *ops,
            inode_id_t *free_inode_id) -> ChfsResult<inode_id_t> {
        // use alloc a block
        block_id_t free_block = 0;
        auto block_id_res = this->block_allocator_->allocate(ops, &free_block);
        if (block_id_res.is_err() && !ops) {
            return block_id_res.unwrap_error();
        } else if (block_id_res.is_ok()) {
            free_block = block_id_res.unwrap();
        }

        // alloc an inode
        inode_id_t inode_id = 0;
        auto inode_id_res =
                inode_manager_->allocate_inode(type, free_block, ops, &inode_id);
        if (inode_id_res.is_err() && !ops) {
            return {inode_id_res.unwrap_error()};
        } else if (inode_id_res.is_ok()) {
            inode_id = inode_id_res.unwrap();
        }

        if (free_inode_id) *free_inode_id = inode_id;
        if (block_id_res.is_err()) {
            return {block_id_res.unwrap_error()};
        }
        if (inode_id_res.is_err()) {
            return {inode_id_res.unwrap_error()};
        }
        return {inode_id};
    }

    auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr> {
        return this->inode_manager_->get_attr(id);
    }

    auto FileOperation::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
        return this->inode_manager_->get_type_attr(id);
    }

    auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType> {
        return this->inode_manager_->get_type(id);
    }

    auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64 {
        return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
    }

    auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                         u64 offset) -> ChfsResult<u64> {
        auto read_res = this->read_file(id);
        if (read_res.is_err()) {
            return ChfsResult<u64>(read_res.unwrap_error());
        }

        auto content = read_res.unwrap();
        if (offset + sz > content.size()) {
            content.resize(offset + sz);
        }
        memcpy(content.data() + offset, data, sz);
        auto write_res = this->write_file(id, content, nullptr);
        if (write_res.is_err()) {
            return ChfsResult<u64>(write_res.unwrap_error());
        }
        return ChfsResult<u64>(sz);
    }

// {Your code here}
    auto FileOperation::write_file(
            inode_id_t id, const std::vector<u8> &content,
            std::vector<std::shared_ptr<BlockOperation>> *ops) -> ChfsNullResult {
        auto error_code = ErrorType::DONE;
        bool has_error = false;
        const auto block_size = this->block_manager_->block_size();
        usize old_block_num = 0;
        usize new_block_num = 0;
        u64 original_file_sz = 0;
        // 1. read the inode
        std::vector<u8> inode(block_size);
        std::vector<u8> indirect_block(0);
        indirect_block.reserve(block_size);

        auto inode_p = reinterpret_cast<Inode *>(inode.data());
        auto inlined_blocks_num = 0;

        auto inode_res = this->inode_manager_->read_inode(id, inode);
        if (inode_res.is_err()) {
            error_code = inode_res.unwrap_error();
            // I know goto is bad, but we have no choice
            goto err_ret;
        } else {
            inlined_blocks_num = inode_p->get_direct_block_num();
        }

        if (content.size() > inode_p->max_file_sz_supported()) {
            std::cerr << "file size too large: " << content.size() << " vs. "
                      << inode_p->max_file_sz_supported() << std::endl;
            error_code = ErrorType::OUT_OF_RESOURCE;
            goto err_ret;
        }

        // 2. make sure whether we need to allocate more blocks
        original_file_sz = inode_p->get_size();
        old_block_num = calculate_block_sz(original_file_sz, block_size);
        new_block_num = calculate_block_sz(content.size(), block_size);

        if (new_block_num > old_block_num) {
            // If we need to allocate more blocks.
            for (usize idx = old_block_num; idx < new_block_num; ++idx) {
                if (inode_p->is_direct_block(idx)) {
                    // direct block
                    block_id_t free_block = 0;
                    auto block_id_res = this->block_allocator_->allocate(ops, &free_block);
                    if (block_id_res.is_err() && !ops) {
                        error_code = block_id_res.unwrap_error();
                        goto err_ret;
                    }
                    //        free_block = block_id_res.unwrap();
                    inode_p->set_block_direct(idx, free_block);
                } else {
                    // indirect block
                    // get the indirect block id
                    auto block_id_res =
                            inode_p->get_or_insert_indirect_block(this->block_allocator_);
                    if (block_id_res.is_err()) {
                        error_code = block_id_res.unwrap_error();
                        goto err_ret;
                    }
                    auto indirect_block_id = block_id_res.unwrap();
                    // get the indirect inode info
                    if (indirect_block.empty()) {
                        indirect_block.resize(block_size);
                        block_manager_->read_block(indirect_block_id, indirect_block.data());
                    }
                    auto indirect_inode = reinterpret_cast<Inode *>(indirect_block.data());
                    // allocate a new block for storing info
                    auto indirect_block_id_res =
                            this->block_allocator_->allocate(ops, nullptr);
                    if (indirect_block_id_res.is_err()) {
                        error_code = indirect_block_id_res.unwrap_error();
                        goto err_ret;
                    }
                    auto free_block = indirect_block_id_res.unwrap();
                    // fill the block id
                    indirect_inode->set_block_direct(idx - inlined_blocks_num, free_block);
                }
            }
        } else {
            // check the free inode number
            // We need to free the extra blocks.
            for (block_id_t idx = new_block_num; idx < old_block_num; ++idx) {
                if (inode_p->is_direct_block(idx)) {
                    auto res =
                            this->block_allocator_->deallocate(inode_p->blocks[idx], ops);
                    if (res.is_err()) {
                        error_code = res.unwrap_error();
                        goto err_ret;
                    }
                    inode_p->blocks[idx] = KInvalidBlockID;
                } else {
                    Inode *indirect_inode_ = nullptr;
                    if (!indirect_block.empty()) {
                        indirect_inode_ = reinterpret_cast<Inode *>(indirect_block.data());
                    } else {
                        indirect_block.resize(block_size);
                        auto indirect_block_res = this->block_manager_->read_block(
                                inode_p->get_indirect_block_id(), indirect_block.data());
                        if (indirect_block_res.is_err()) {
                            error_code = indirect_block_res.unwrap_error();
                            goto err_ret;
                        }
                        indirect_inode_ = reinterpret_cast<Inode *>(indirect_block.data());
                    }
                    // deallocate the block
                    auto res = this->block_allocator_->deallocate(
                            indirect_inode_->blocks[idx - inlined_blocks_num], ops);
                    if (res.is_err()) {
                        error_code = res.unwrap_error();
                        goto err_ret;
                    }
                    indirect_inode_->blocks[idx - inlined_blocks_num] = KInvalidBlockID;
                }
            }

            // If there are no more indirect blocks.
            if (old_block_num > inlined_blocks_num &&
                new_block_num <= inlined_blocks_num) {
                // deallocate the indirect block
                auto res = this->block_allocator_->deallocate(
                        inode_p->get_indirect_block_id(), ops);
                if (res.is_err()) {
                    error_code = res.unwrap_error();
                    goto err_ret;
                }
                inode_p->invalid_indirect_block_id();
                indirect_block.clear();
                indirect_block.resize(0);
            }
        }

        // 3. write the contents
        inode_p->inner_attr.size = content.size();
        inode_p->inner_attr.mtime = time(0);

        {
            // inner block id
            auto block_idx = 0;
            u64 write_sz = 0;

            while (write_sz < content.size()) {
                auto sz = ((content.size() - write_sz) > block_size)
                          ? block_size
                          : (content.size() - write_sz);
                std::vector<u8> buffer(block_size);
                memcpy(buffer.data(), content.data() + write_sz, sz);
                block_id_t w_block_id = 0;
                if (inode_p->is_direct_block(block_idx)) {
                    // get block id of current direct block.
                    w_block_id = inode_p->blocks[block_idx];
                } else {
                    // get block id of current indirect block.
                    // translate to inode
                    if (!indirect_block.empty()) {
                        auto *indirect_inode =
                                reinterpret_cast<Inode *>(indirect_block.data());
                        w_block_id = indirect_inode->blocks[block_idx - inlined_blocks_num];
                    } else {
                        indirect_block.resize(block_size);
                        auto indirect_block_res = this->block_manager_->read_block(
                                inode_p->get_indirect_block_id(), indirect_block.data());
                        if (indirect_block_res.is_err()) {
                            error_code = indirect_block_res.unwrap_error();
                            goto err_ret;
                        }
                        auto *indirect_inode =
                                reinterpret_cast<Inode *>(indirect_block.data());
                        w_block_id = indirect_inode->blocks[block_idx - inlined_blocks_num];
                    }
                }

                auto write_res = this->block_manager_->write_partial_block(
                        w_block_id, buffer.data(), 0, sz);
                if (write_res.is_err()) {
                    error_code = write_res.unwrap_error();
                    goto err_ret;
                }
                std::vector<u8> b_buffer(block_size);
                block_manager_->read_block(w_block_id, b_buffer.data());
                if (ops)
                    ops->emplace_back(
                            std::make_shared<BlockOperation>(w_block_id, b_buffer));
                write_sz += sz;
                block_idx += 1;
            }
        }

        // finally, update the inode
        {
            inode_p->inner_attr.set_all_time(time(0));

            auto write_res =
                    this->block_manager_->write_block(inode_res.unwrap(), inode.data());
            if (ops)
                ops->emplace_back(
                        std::make_shared<BlockOperation>(inode_res.unwrap(), inode));
            if (write_res.is_err()) {
                error_code = write_res.unwrap_error();
                if (!ops)
                    goto err_ret;
                else {
                    has_error = true;
                }
            }
            if (!indirect_block.empty()) {
                write_res = inode_p->write_indirect_block(this->block_manager_,
                                                          indirect_block, ops);
                if (write_res.is_err()) {
                    error_code = write_res.unwrap_error();
                    if (!ops) {
                        goto err_ret;
                    } else {
                        has_error = true;
                    }
                }
            }
        }
        if (has_error) goto err_ret;

        return KNullOk;

        err_ret:
        return ChfsNullResult(error_code);
    }

// {Your code here}
    auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8>> {
        auto error_code = ErrorType::DONE;
        std::vector<u8> content;

        const auto block_size = this->block_manager_->block_size();

        // 1. read the inode
        std::vector<u8> inode(block_size);
        std::vector<u8> indirect_block(0);
        indirect_block.reserve(block_size);

        auto inode_p = reinterpret_cast<Inode *>(inode.data());
        u64 file_sz = 0;
        u64 read_sz = 0;
        auto block_idx = 0;
        auto inline_blocks_num = 0;

        auto inode_res = this->inode_manager_->read_inode(id, inode);
        if (inode_res.is_err()) {
            error_code = inode_res.unwrap_error();
            // I know goto is bad, but we have no choice
            goto err_ret;
        } else {
            inline_blocks_num = inode_p->get_direct_block_num();
        }

        file_sz = inode_p->get_size();
        content.reserve(file_sz);

        // Now read the file
        while (read_sz < file_sz) {
            auto sz = ((inode_p->get_size() - read_sz) > block_size)
                      ? block_size
                      : (inode_p->get_size() - read_sz);
            std::vector<u8> buffer(block_size);

            block_id_t r_block_id;
            // Get current block id.
            if (inode_p->is_direct_block(block_idx)) {
                // the case of direct block.
                r_block_id = inode_p->blocks[block_idx];
            } else {
                // the case of indirect block
                auto indirect_block_id = inode_p->get_indirect_block_id();
                if (indirect_block.empty()) {
                    indirect_block.resize(block_size);
                    auto indirect_block_res = this->block_manager_->read_block(
                            indirect_block_id, indirect_block.data());
                    if (indirect_block_res.is_err()) {
                        error_code = indirect_block_res.unwrap_error();
                        goto err_ret;
                    }
                }
                // translate to inode
                auto *indirect_inode = reinterpret_cast<Inode *>(indirect_block.data());
                r_block_id = indirect_inode->blocks[block_idx - inline_blocks_num];
            }

            // read from current block and store to `content`.
            auto read_res = this->block_manager_->read_block(r_block_id, buffer.data());
            if (read_res.is_err()) {
                error_code = read_res.unwrap_error();
                goto err_ret;
            }

            content.insert(content.end(), buffer.begin(), buffer.begin() + sz);
            read_sz += sz;
            block_idx += 1;
        }

        return ChfsResult<std::vector<u8>>(std::move(content));

        err_ret:
        return ChfsResult<std::vector<u8>>(error_code);
    }

    auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
    -> ChfsResult<std::vector<u8>> {
        auto res = read_file(id);
        if (res.is_err()) {
            return res;
        }

        auto content = res.unwrap();
        return ChfsResult<std::vector<u8>>(
                std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
    }

    auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr> {
        auto attr_res = this->getattr(id);
        if (attr_res.is_err()) {
            return ChfsResult<FileAttr>(attr_res.unwrap_error());
        }

        auto attr = attr_res.unwrap();
        auto file_content = this->read_file(id);
        if (file_content.is_err()) {
            return ChfsResult<FileAttr>(file_content.unwrap_error());
        }

        auto content = file_content.unwrap();

        if (content.size() != sz) {
            content.resize(sz);

            auto write_res = this->write_file(id, content, nullptr);
            if (write_res.is_err()) {
                return ChfsResult<FileAttr>(write_res.unwrap_error());
            }
        }

        attr.size = sz;
        return ChfsResult<FileAttr>(attr);
    }

}  // namespace chfs
