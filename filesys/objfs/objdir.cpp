
#include "objfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;

ObjDirectoryIterator::ObjDirectoryIterator(
    std::shared_ptr<ObjFilesystem> fs, FileId fileid)
    : fs_(fs),
      iterator_(fs->db()->iterator(fs->directoriesNS())),
      start_(fileid, ""),
      end_(FileId(fileid + 1), "")
{
    iterator_->seek(start_);
    decodeEntry();
}

bool ObjDirectoryIterator::valid() const
{
    return iterator_->valid(end_);
}

FileId ObjDirectoryIterator::fileid() const
{
    return FileId(entry_.fileid);
}

std::string ObjDirectoryIterator::name() const
{
    auto key = iterator_->key();
    return string(
        reinterpret_cast<const char*>(key->data() + sizeof(uint64_t)),
        key->size() - sizeof(uint64_t));
}

std::shared_ptr<File> ObjDirectoryIterator::file() const
{
    if (!file_)
        file_ = fs_->find(fileid());
    return file_;
}

void ObjDirectoryIterator::next()
{
    file_.reset();
    iterator_->next();
    decodeEntry();
}

void ObjDirectoryIterator::decodeEntry()
{
    if (valid()) {
        auto val = iterator_->value();
        oncrpc::XdrMemory xm(val->data(), val->size());
        xdr(entry_, static_cast<oncrpc::XdrSource*>(&xm));
    }
}
