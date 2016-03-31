// -*- c++ -*-
#pragma once

namespace filesys {
namespace objfs {

/// Key type for our DB - we index by fileid, using fileid zero for filesystem
/// metadata. The fileid is encoded big endian to group consecutive fileids
struct KeyType {
    KeyType(std::uint64_t id)
        : buf_(std::make_shared<oncrpc::Buffer>(sizeof(std::uint64_t)))
    {
        std::uint64_t n = id;
        for (int i = 0; i < sizeof(n); i++) {
            buf_->data()[i] = (n >> 56) & 0xff;
            n <<= 8;
        }
    }

    KeyType(std::shared_ptr<oncrpc::Buffer> buf)
        : buf_(buf)
    {
    }

    operator std::shared_ptr<oncrpc::Buffer>() const
    {
        return buf_;
    }

    std::uint64_t fileid() const
    {
        std::uint64_t n = 0;
        for (int i = 0; i < sizeof(n); i++) {
            n = (n << 8) + buf_->data()[i];
        }
        return n;
    }

private:
    std::shared_ptr<oncrpc::Buffer> buf_;
};

/// Key type for directory entries - we append the name to the fileid
/// which groups the keys for efficient lookup. We use big endian for
/// the fileid so we can iterate over the directory easily
struct DirectoryKeyType
{
    DirectoryKeyType(std::uint64_t id, std::string name)
        : buf_(std::make_shared<oncrpc::Buffer>(sizeof(std::uint64_t) + name.size()))
    {
        std::uint64_t n = id;
        for (int i = 0; i < sizeof(n); i++) {
            buf_->data()[i] = (n >> 56) & 0xff;
            n <<= 8;
        }
        std::copy_n(
            reinterpret_cast<const uint8_t*>(name.data()), name.size(),
            buf_->data() + sizeof(uint64_t));
    }

    DirectoryKeyType(std::shared_ptr<oncrpc::Buffer> buf)
        : buf_(buf)
    {
    }

    operator std::shared_ptr<oncrpc::Buffer>() const
    {
        return buf_;
    }

    std::uint64_t fileid() const
    {
        std::uint64_t n = 0;
        for (int i = 0; i < sizeof(n); i++) {
            n = (n << 8) + buf_->data()[i];
        }
        return n;
    }

    std::string name() const
    {
        return std::string(
            reinterpret_cast<const char*>(buf_->data() + sizeof(uint64_t)),
            buf_->size() - sizeof(uint64_t));
    }

private:
    std::shared_ptr<oncrpc::Buffer> buf_;
};

/// Key type for file data and block map - we index by fileid and byte offset,
/// using the highest bit of the file offset to distinguish between data and
/// block map
struct DataKeyType {
    DataKeyType(std::uint64_t id, std::uint64_t off)
        : buf_(std::make_shared<oncrpc::Buffer>(2*sizeof(std::uint64_t)))
    {
        std::uint64_t n = id;
        for (int i = 0; i < sizeof(n); i++) {
            buf_->data()[i] = (n >> 56) & 0xff;
            n <<= 8;
        }
        n = off;
        for (int i = 0; i < sizeof(n); i++) {
            buf_->data()[8+i] = (n >> 56) & 0xff;
            n <<= 8;
        }
    }

    DataKeyType(std::shared_ptr<oncrpc::Buffer> buf)
        : buf_(buf)
    {
        std::copy_n(buf->data(), buf->size(), buf_->data());
    }

    operator std::shared_ptr<oncrpc::Buffer>() const
    {
        return buf_;
    }

    std::uint64_t fileid() const
    {
        std::uint64_t n = 0;
        for (int i = 0; i < sizeof(n); i++) {
            n = (n << 8) + buf_->data()[i];
        }
        return n;
    }

    std::uint64_t offset() const
    {
        std::uint64_t n = 0;
        for (int i = 0; i < sizeof(n); i++) {
            n = (n << 8) + buf_->data()[8+i];
        }
        return n;
    }

private:
    std::shared_ptr<oncrpc::Buffer> buf_;
};

}
}
