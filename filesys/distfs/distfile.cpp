/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include "distfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace filesys::distfs;
using namespace keyval;
using namespace std;

DistFile::DistFile(std::shared_ptr<ObjFilesystem> fs, FileId fileid)
    : ObjFile(fs, fileid)
{
}

DistFile::DistFile(shared_ptr<ObjFilesystem> fs, ObjFileMetaImpl&& meta)
    : ObjFile(fs, move(meta))
{
}

DistFile::~DistFile()
{
}

shared_ptr<Fsattr> DistFile::fsstat(const Credential& cred)
{
    unique_lock<mutex> lock(mutex_);
    checkAccess(cred, AccessFlags::READ);
    auto fs = dynamic_pointer_cast<DistFilesystem>(fs_.lock());
    return make_shared<DistFsattr>(fs->storage());
}

shared_ptr<Piece> DistFile::data(
    const Credential& cred, std::uint64_t offset, bool forWriting)
{
    if (meta_.attr.type != PT_REG)
        throw system_error(EINVAL, system_category());

    auto lk = lock();

    auto fs = dynamic_pointer_cast<DistFilesystem>(fs_.lock());
    auto blockSize = meta_.blockSize;
    auto bn = blockSize ? offset / blockSize : 0;
    auto off = bn * blockSize;

    unique_ptr<keyval::Transaction> trans;
    if (forWriting) {
        trans = fs->db()->beginTransaction();
        updateModifyTime();
        writeMeta(trans.get());
    }

    auto piece = fs->findPiece(
        PieceId{fileid(), off, blockSize}, forWriting, trans.get());

    lk.unlock();
    if (forWriting) {
        fs->db()->commit(move(trans));
    }

    return piece;
}

void DistFile::truncate(
    const Credential& cred, Transaction* trans, uint64_t newSize)
{
    // If the file size is reduced, purge any data after the new size.
    auto fs = dynamic_pointer_cast<DistFilesystem>(fs_.lock());
    auto blockSize = meta_.blockSize;
    auto blockMask = blockSize - 1;

    PieceData start(
        fileid(), (newSize + blockMask) & ~blockMask, blockSize);
    DataKeyType end(fileid(), ~0ull);

    if (blockSize || newSize == 0) {
        auto iterator = fs->dataNS()->iterator(start);
        while (iterator->valid(end)) {
            PieceData dk(iterator->key());
            fs->removePiece(
                cred, PieceId{dk.fileid(), dk.offset(), dk.size()}, trans);
            trans->remove(fs->dataNS(), iterator->key());
            iterator->next();
        }
    }
    meta_.attr.size = newSize;

    if (newSize > 0) {
        // Possibly truncate the last piece of the file
        auto bn = blockSize ? newSize / blockSize : 0;
        auto boff = blockSize ? newSize % blockSize : newSize;

        try {
            auto off = bn * blockSize;
            auto piece = fs->findPiece(
                PieceId{fileid(), off, blockSize}, false, nullptr);
            piece->truncate(cred, off + boff, trans);
        }
        catch (system_error&) {
        }
    }
}

DistOpenFile::~DistOpenFile()
{
    for (auto piece: pieces_)
        piece->close();
}

shared_ptr<Buffer> DistOpenFile::read(
    std::uint64_t offset, std::uint32_t len, bool& eof)
{
    auto lk = file_->lock();

    file_->checkAccess(cred_, AccessFlags::READ);
    file_->updateAccessTime();
    file_->writeMeta();
    auto& meta = file_->meta();

    auto fs = dynamic_pointer_cast<DistFilesystem>(file_->ofs());
    auto blockSize = meta.blockSize;
    auto bn = blockSize ? offset / blockSize : 0;
    auto boff = blockSize ? offset % blockSize : offset;
    eof = false;
    if (offset >= meta.attr.size) {
        eof = true;
        return make_shared<oncrpc::Buffer>(0);
    }
    if (offset + len >= meta.attr.size) {
        eof = true;
        len = meta.attr.size - offset;
    }

    lk.unlock();

    // Read one piece at a time and copy out to buffer
    auto res = make_shared<oncrpc::Buffer>(len);
    for (uint32_t i = 0; i < len; ) {
        auto off = bn * blockSize;
        try {
            // If the piece exists copy out to buffer
            auto piece = fs->findPiece(
                PieceId{file_->fileid(), off, blockSize}, false, nullptr);
            pieces_.insert(piece);
            auto blen = blockSize ? blockSize - boff : len - i;
            if (i + blen > len) {
                blen = len - i;
            }

            // We use a sparse mapping scheme for the pieces which
            // implies that the write offset within a piece is the
            // same as the offset within the file itself
            auto block = piece->read(cred_, off + boff, blen);
            auto bsz = block->size();
            // XXX optimise for the case where we make exactly one
            // read - we can avoid the copy and just return block.
            copy_n(block->data(), bsz, res->data() + i);
            if (block->size() < blen)
                fill_n(res->data() + i + bsz, blen - bsz, 0);

            i += blen;
        }
        catch (system_error&) {
            // otherwise copy zeros
            auto blen = blockSize ? blockSize - boff : len - i;
            if (i + blen > len) {
                blen = len - i;
            }
            fill_n(res->data() + i, blen, 0);
            i += blen;
        }

        boff = 0;
        bn++;
    }

    return res;
}

uint32_t DistOpenFile::write(
    uint64_t offset, shared_ptr<Buffer> data)
{
    auto lk = file_->lock();

    file_->checkAccess(cred_, AccessFlags::WRITE);
    auto& meta = file_->meta();

    auto fs = dynamic_pointer_cast<DistFilesystem>(file_->ofs());
    auto blockSize = meta.blockSize;
    auto bn = blockSize ? offset / blockSize : 0;
    auto boff = blockSize ? offset % blockSize : offset;
    auto len = data->size();
    auto trans = fs->db()->beginTransaction();
    file_->updateModifyTime();
    file_->writeMeta(trans.get());

    if (offset + len > meta.attr.size) {
        file_->truncate(cred_, trans.get(), offset + len);
    }

    // Write one piece at a time
    for (size_t i = 0; i < len; ) {
        auto off = bn * blockSize;
        auto blen = blockSize ? blockSize - boff : len - i;
        if (i + blen > len)
            blen = len - i;

        // Make a sub-buffer containing the data for this piece. We
        // use a sparse mapping scheme for the pieces which implies
        // that the write offset within a piece is the same as the
        // offset within the file itself
        auto buf = make_shared<Buffer>(data, i, i + blen);
        auto piece = fs->findPiece(
            PieceId{file_->fileid(), off, blockSize}, true, trans.get());
        pieces_.insert(piece);
        piece->write(cred_, off + boff, buf, trans.get());

        // Set up for the next block - note that only the first block can
        // be at a non-zero offset within the block
        i += blen;
        boff = 0;
        bn++;
    }
    needFlush_ = true;
    lk.unlock();

    fs->db()->commit(move(trans));
    return len;
}

void DistOpenFile::flush()
{
    auto lk = file_->lock();
    if (needFlush_) {
        needFlush_ = false;
        lk.unlock();
        file_->ofs()->db()->flush();
    }
}
