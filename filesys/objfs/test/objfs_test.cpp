#include "filesys/objfs/objfs.h"
#include <gtest/gtest.h>

using namespace filesys;
using namespace filesys::objfs;
using namespace std;
using namespace testing;

class ObjfsTest: public ::testing::Test
{
public:
    ObjfsTest()
    {
        system("rm -rf ./testdb");
        fs_ = make_shared<ObjFilesystem>("testdb");
    }

    shared_ptr<ObjFilesystem> fs_;
};

TEST_F(ObjfsTest, Init)
{
    auto root = fs_->root();
    EXPECT_EQ(1, int(root->getattr()->fileid()));
    EXPECT_EQ(2, root->getattr()->nlink());
    EXPECT_EQ(root, root->lookup("."));
    EXPECT_EQ(root, root->lookup(".."));
}

TEST_F(ObjfsTest, Open)
{
    auto root = fs_->root();
    EXPECT_THROW(root->open("foo", 0, [](auto){}), system_error);
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    EXPECT_EQ(0666, file->getattr()->mode());
    EXPECT_EQ(file, root->open("foo", OpenFlags::RDWR, [](auto){}));
    EXPECT_THROW(root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE+OpenFlags::EXCLUSIVE,
        [](auto){}),
        system_error);
}

TEST_F(ObjfsTest, ReadWrite)
{
    auto root = fs_->root();
    auto blockSize = fs_->blockSize();
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    uint8_t buf[] = {'f', 'o', 'o'};

    // Write at start
    file->write(0, make_shared<oncrpc::Buffer>(3, buf));
    EXPECT_EQ(3, file->getattr()->size());
    bool eof;
    EXPECT_EQ("foo", file->read(0, 3, eof)->toString());
    EXPECT_EQ(true, eof);

    // Extend by one block, writing over the block boundary
    file->write(blockSize - 1, make_shared<oncrpc::Buffer>(3, buf));
    EXPECT_EQ(blockSize + 2, file->getattr()->size());
    EXPECT_EQ("foo", file->read(4095, 3, eof)->toString());
    EXPECT_EQ(true, eof);

    // Extend with a hole
    file->write(4*blockSize, make_shared<oncrpc::Buffer>(3, buf));
    EXPECT_EQ(4*blockSize + 3, file->getattr()->size());
    EXPECT_EQ("foo", file->read(4*blockSize, 3, eof)->toString());
    EXPECT_EQ(true, eof);

    // Make sure the hole reads as zero
    auto block = file->read(3*blockSize, blockSize, eof);
    EXPECT_EQ(blockSize, block->size());
    for (int i = 0; i < blockSize; i++)
        EXPECT_EQ(0, block->data()[i]);
    EXPECT_EQ(false, eof);
}

TEST_F(ObjfsTest, Truncate)
{
    auto root = fs_->root();
    auto blockSize = fs_->blockSize();
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    auto buf = make_shared<oncrpc::Buffer>(blockSize);
    fill_n(buf->data(), blockSize, 1);
    for (int i = 0; i < 10; i++)
        file->write(i * blockSize, buf);
    EXPECT_GE(10 * blockSize, file->getattr()->used());
    file->setattr([](auto attr){ attr->setSize(0); });
    file->setattr([=](auto attr){ attr->setSize(blockSize); });
    bool eof;
    EXPECT_EQ(0, file->read(0, 1, eof)->data()[0]);
}

TEST_F(ObjfsTest, Mtime)
{
    auto root = fs_->root();
    auto blockSize = fs_->blockSize();
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    auto buf = make_shared<oncrpc::Buffer>(blockSize);
    fill_n(buf->data(), blockSize, 1);
    auto mtime = file->getattr()->atime();
    file->write(0, buf);
    EXPECT_LT(mtime, file->getattr()->mtime());
}

TEST_F(ObjfsTest, Atime)
{
    auto root = fs_->root();
    auto blockSize = fs_->blockSize();
    auto file = root->open(
        "foo", OpenFlags::RDWR+OpenFlags::CREATE,
        [](auto attr){ attr->setMode(0666); });
    auto buf = make_shared<oncrpc::Buffer>(blockSize);
    fill_n(buf->data(), blockSize, 1);
    file->write(0, buf);
    auto atime = file->getattr()->atime();
    bool eof;
    file->read(0, blockSize, eof);
    EXPECT_LT(atime, file->getattr()->atime());
}

TEST_F(ObjfsTest, Symlink)
{
    auto root = fs_->root();
    auto link = root->symlink("foo", "bar", [](auto){});
    EXPECT_EQ(1, link->getattr()->nlink());
    EXPECT_EQ(FileType::SYMLINK, link->getattr()->type());
    EXPECT_EQ(3, link->getattr()->size());
    EXPECT_EQ("bar", link->readlink());
    EXPECT_EQ(3, root->getattr()->size());
}

TEST_F(ObjfsTest, Mkfifo)
{
    auto root = fs_->root();
    auto fifo = root->mkfifo("foo", [](auto){});
    EXPECT_EQ(1, fifo->getattr()->nlink());
    EXPECT_EQ(FileType::FIFO, fifo->getattr()->type());
    EXPECT_EQ(3, root->getattr()->size());
}

TEST_F(ObjfsTest, Mkdir)
{
    auto root = fs_->root();
    auto dir = root->mkdir("foo", [](auto attr){ attr->setMode(0700); });
    EXPECT_EQ(2, dir->getattr()->nlink());
    EXPECT_EQ(0700, dir->getattr()->mode());
    EXPECT_EQ(3, root->getattr()->nlink());
    EXPECT_EQ(dir, dir->lookup("."));
    EXPECT_EQ(root, dir->lookup(".."));
    EXPECT_EQ(3, root->getattr()->size());
}

TEST_F(ObjfsTest, Remove)
{
    auto root = fs_->root();
    auto link = root->symlink("foo", "bar", [](auto){});
    auto dir = root->mkdir("baz", [](auto){});
    EXPECT_THROW(root->remove("baz"), system_error);
    root->remove("foo");
    EXPECT_EQ(0, link->getattr()->nlink());
    EXPECT_EQ(3, root->getattr()->size());
}

TEST_F(ObjfsTest, Rmdir)
{
    auto root = fs_->root();
    auto dir = root->mkdir("foo", [](auto){});
    root->rmdir("foo");
    EXPECT_EQ(0, int(dir->getattr()->nlink()));
    EXPECT_EQ(2, int(root->getattr()->nlink()));
    EXPECT_EQ(2, root->getattr()->size());
    EXPECT_THROW(root->lookup("foo"), system_error);

    root->symlink("foo", "bar", [](auto){});
    EXPECT_THROW(root->rmdir("foo"), system_error);
}

TEST_F(ObjfsTest, RenameFile)
{
    auto root = fs_->root();
    auto file = root->mkfifo("foo", [](auto){});

    // Simple rename within root directory
    root->rename("bar", root, "foo");
    EXPECT_THROW(root->lookup("foo"), system_error);
    EXPECT_EQ(file, root->lookup("bar"));
    EXPECT_EQ(3, root->getattr()->size());

    // Rename to a different directory
    auto dir = root->mkdir("foo", [](auto){});
    EXPECT_EQ(4, root->getattr()->size());
    dir->rename("bar", root, "bar");
    EXPECT_THROW(root->lookup("bar"), system_error);
    EXPECT_EQ(file, dir->lookup("bar"));
    EXPECT_EQ(3, root->getattr()->size());
    EXPECT_EQ(3, dir->getattr()->size());

    // Renaming to an existing name should delete the target first ...
    auto file2 = dir->mkfifo("foo", [](auto){});
    EXPECT_EQ(4, dir->getattr()->size());
    dir->rename("foo", dir, "bar");
    EXPECT_EQ(0, file2->getattr()->nlink());
    EXPECT_EQ(file, dir->lookup("foo"));
    EXPECT_EQ(3, dir->getattr()->size());

    // ... unless the target is a non-empty directory
    dir->mkdir("bar", [](auto){})->mkfifo("aaa", [](auto){});
    EXPECT_EQ(4, dir->getattr()->size());
    EXPECT_THROW(dir->rename("bar", dir, "foo"), system_error);

    // Check that the rename succeeds when the target is empty and verify
    // the size and link count
    dir->lookup("bar")->remove("aaa");
    EXPECT_EQ(3, dir->getattr()->nlink());
    dir->rename("bar", dir, "foo");
    EXPECT_EQ(2, dir->getattr()->nlink());
    EXPECT_EQ(3, dir->getattr()->size());
}

TEST_F(ObjfsTest, RenameDir)
{
    auto root = fs_->root();
    auto dir = root->mkdir("foo", [](auto){});

    // Simple rename within root directory
    root->rename("bar", root, "foo");
    EXPECT_THROW(root->lookup("foo"), system_error);
    EXPECT_EQ(dir, root->lookup("bar"));

    // Rename to a different directory
    auto dir2 = root->mkdir("foo", [](auto){});
    EXPECT_EQ(4, root->getattr()->nlink());
    EXPECT_EQ(2, dir->getattr()->nlink());
    dir->rename("foo", root, "foo");
    EXPECT_EQ(3, root->getattr()->nlink());
    EXPECT_EQ(3, dir->getattr()->nlink());
    EXPECT_THROW(root->lookup("foo"), system_error);
    EXPECT_EQ(dir2, dir->lookup("foo"));
    EXPECT_EQ(dir, dir2->lookup(".."));
}
