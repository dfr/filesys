#pragma once

#include <fs++/filesys.h>
#include <gtest/gtest.h>

static inline void setMode666(filesys::Setattr* attr)
{
    attr->setMode(0666);
}

static inline void setMode777(filesys::Setattr* attr)
{
    attr->setMode(0777);
}

template <typename T>
class FilesystemTest: public ::testing::Test, public T
{
public:
    FilesystemTest()
        : cred_(0, 0, {})
    {
    }

    filesys::Credential cred_;
    //std::shared_ptr<filesys::Filesystem> fs_;
    //size_t blockSize_;
};

TYPED_TEST_CASE_P(FilesystemTest);

TYPED_TEST_P(FilesystemTest, Init)
{
    auto root = this->fs_->root();
    EXPECT_EQ(1, int(root->getattr()->fileid()));
    EXPECT_EQ(2, root->getattr()->nlink());
    EXPECT_EQ(root, root->lookup(this->cred_, "."));
    EXPECT_EQ(root, root->lookup(this->cred_, ".."));
}

TYPED_TEST_P(FilesystemTest, Access)
{
    using filesys::AccessFlags;
    auto root = this->fs_->root();
    EXPECT_EQ(true,
        root->access(
            this->cred_,
            AccessFlags::READ+AccessFlags::WRITE+AccessFlags::EXECUTE));
    root->setattr(this->cred_, setMode666);
    EXPECT_EQ(false,
        root->access(
            this->cred_,
            AccessFlags::EXECUTE));
}

TYPED_TEST_P(FilesystemTest, Open)
{
    using filesys::OpenFlags;
    auto root = this->fs_->root();
    EXPECT_THROW(
        root->open(this->cred_, "foo", OpenFlags::READ, setMode666),
        std::system_error);
    auto of = root->open(
        this->cred_, "foo", OpenFlags::RDWR+OpenFlags::CREATE, setMode666);
    auto file = of->file();
    EXPECT_EQ(0666, file->getattr()->mode());
    EXPECT_EQ(
        file,
        root->open(this->cred_, "foo", OpenFlags::RDWR, setMode666)->file());
    EXPECT_THROW(root->open(
        this->cred_, "foo",
        OpenFlags::RDWR+OpenFlags::CREATE+OpenFlags::EXCLUSIVE,
        setMode666),
        std::system_error);
    root->open(
        this->cred_, "foo2",
        OpenFlags::RDWR+OpenFlags::CREATE+OpenFlags::EXCLUSIVE,
        setMode666);
}

TYPED_TEST_P(FilesystemTest, ReadWrite)
{
    using filesys::Buffer;
    using filesys::OpenFlags;
    using std::make_shared;

    auto root = this->fs_->root();
    auto of = root->open(
        this->cred_, "foo", OpenFlags::RDWR+OpenFlags::CREATE, setMode666);
    uint8_t buf[] = {'f', 'o', 'o'};

    // Write at start
    of->write(0, make_shared<Buffer>(3, buf));
    EXPECT_EQ(3, of->file()->getattr()->size());
    bool eof;
    EXPECT_EQ("foo", of->read(0, 3, eof)->toString());
    EXPECT_EQ(true, eof);

    // Extend by one block, writing over the block boundary
    of->write(this->blockSize_ - 1, make_shared<Buffer>(3, buf));
    EXPECT_EQ(this->blockSize_ + 2, of->file()->getattr()->size());
    EXPECT_EQ("foo", of->read(this->blockSize_ - 1, 3, eof)->toString());
    EXPECT_EQ(true, eof);

    // Extend with a hole
    of->write(4*this->blockSize_, make_shared<Buffer>(3, buf));
    EXPECT_EQ(4*this->blockSize_ + 3, of->file()->getattr()->size());
    EXPECT_EQ("foo", of->read(4*this->blockSize_, 3, eof)->toString());
    EXPECT_EQ(true, eof);

    // Make sure the hole reads as zero
    auto block = of->read(3*this->blockSize_, this->blockSize_, eof);
    EXPECT_EQ(this->blockSize_, block->size());
    for (int i = 0; i < this->blockSize_; i++)
        EXPECT_EQ(0, block->data()[i]);
    EXPECT_EQ(false, eof);
}

TYPED_TEST_P(FilesystemTest, Truncate)
{
    using filesys::Buffer;
    using filesys::OpenFlags;
    using std::make_shared;
    using namespace std::literals;

    auto root = this->fs_->root();
    auto of = root->open(
        this->cred_, "foo", OpenFlags::RDWR+OpenFlags::CREATE, setMode666);
    auto file = of->file();
    auto buf = make_shared<Buffer>(this->blockSize_);
    std::fill_n(buf->data(), this->blockSize_, 1);
    for (int i = 0; i < 10; i++)
        of->write(i * this->blockSize_, buf);
    EXPECT_GE(10 * this->blockSize_, file->getattr()->used());
    // Force mtime to change, flushing any caches
    *this->clock_ += 1s;
    file->setattr(
        this->cred_, [](auto attr){ attr->setSize(0); });
    file->setattr(
        this->cred_, [=](auto attr){ attr->setSize(this->blockSize_); });
    bool eof;
    EXPECT_EQ(0, of->read(0, 1, eof)->data()[0]);
}

TYPED_TEST_P(FilesystemTest, Mtime)
{
    using filesys::Buffer;
    using filesys::OpenFlags;
    using std::make_shared;
    using namespace std::literals;

    auto root = this->fs_->root();
    auto of = root->open(
        this->cred_, "foo", OpenFlags::RDWR+OpenFlags::CREATE, setMode666);
    auto file = of->file();
    auto buf = make_shared<Buffer>(this->blockSize_);
    std::fill_n(buf->data(), this->blockSize_, 1);
    auto mtime = file->getattr()->mtime();
    *this->clock_ += 1s;
    of->write(0, buf);
    EXPECT_LT(mtime, file->getattr()->mtime());

    // Test mtime for directory modifying operations
    mtime = root->getattr()->mtime();
    *this->clock_ += 1s;
    root->link(this->cred_, "bar", file);
    EXPECT_LT(mtime, root->getattr()->mtime());

    mtime = root->getattr()->mtime();
    *this->clock_ += 1s;
    root->remove(this->cred_, "bar");
    EXPECT_LT(mtime, root->getattr()->mtime());

    mtime = root->getattr()->mtime();
    *this->clock_ += 1s;
    root->mkdir(this->cred_, "bar", setMode777);
    EXPECT_LT(mtime, root->getattr()->mtime());

    mtime = root->getattr()->mtime();
    *this->clock_ += 1s;
    root->rename(this->cred_, "baz", root, "bar");
    EXPECT_LT(mtime, root->getattr()->mtime());

    mtime = root->getattr()->mtime();
    *this->clock_ += 1s;
    root->rmdir(this->cred_, "baz");
    EXPECT_LT(mtime, root->getattr()->mtime());

    // If moving between directories, both change. If the moving object is
    // a directory then it also changes due to ".." rewriting
    auto a = root->mkdir(this->cred_, "a", setMode777);
    auto b = root->mkdir(this->cred_, "b", setMode777);
    auto c = a->mkdir(this->cred_, "c", setMode777);

    // XXX: disable cmtime test for now since NFSv3 doesn't report the
    // moved directory attributes
    auto amtime = a->getattr()->mtime();
    auto bmtime = b->getattr()->mtime();
    //auto cmtime = c->getattr()->mtime();

    *this->clock_ += 1s;
    b->rename(this->cred_, "c", a, "c");

    EXPECT_LT(amtime, a->getattr()->mtime());
    EXPECT_LT(bmtime, b->getattr()->mtime());
    //EXPECT_LT(cmtime, c->getattr()->mtime());
}

TYPED_TEST_P(FilesystemTest, Atime)
{
    using filesys::Buffer;
    using filesys::OpenFlags;
    using std::make_shared;
    using namespace std::literals;

    auto root = this->fs_->root();
    auto of = root->open(
        this->cred_, "foo", OpenFlags::RDWR+OpenFlags::CREATE, setMode666);
    auto file = of->file();
    auto buf = make_shared<Buffer>(this->blockSize_);
    std::fill_n(buf->data(), this->blockSize_, 1);
    of->write(0, buf);
    auto atime = file->getattr()->atime();
    *this->clock_ += 1s;
    bool eof;
    of->read(0, this->blockSize_, eof);
    EXPECT_LT(atime, file->getattr()->atime());

    atime = root->getattr()->atime();
    *this->clock_ += 1s;
    auto iter = root->readdir(this->cred_, 0);
    while (iter->valid()) iter->next();
    EXPECT_LT(atime, root->getattr()->atime());
}

TYPED_TEST_P(FilesystemTest, Symlink)
{
    using filesys::FileType;
    using namespace std::literals;

    auto root = this->fs_->root();
    auto link = root->symlink(this->cred_, "foo", "bar", setMode666);
    EXPECT_EQ(1, link->getattr()->nlink());
    EXPECT_EQ(FileType::SYMLINK, link->getattr()->type());
    EXPECT_EQ(3, link->getattr()->size());
    EXPECT_EQ("bar", link->readlink(this->cred_));
    EXPECT_EQ(3, root->getattr()->size());

    auto atime = link->getattr()->atime();
    *this->clock_ += 1s;
    link->readlink(this->cred_);
    EXPECT_LT(atime, link->getattr()->atime());
}

TYPED_TEST_P(FilesystemTest, Mkfifo)
{
    using filesys::FileType;

    auto root = this->fs_->root();
    auto fifo = root->mkfifo(this->cred_, "foo", setMode666);
    EXPECT_EQ(1, fifo->getattr()->nlink());
    EXPECT_EQ(FileType::FIFO, fifo->getattr()->type());
    EXPECT_EQ(3, root->getattr()->size());
}

TYPED_TEST_P(FilesystemTest, Mkdir)
{
    auto root = this->fs_->root();
    auto dir = root->mkdir(this->cred_, "foo", setMode777);
    EXPECT_EQ(2, dir->getattr()->nlink());
    EXPECT_EQ(0777, dir->getattr()->mode());
    EXPECT_EQ(3, root->getattr()->nlink());
    EXPECT_EQ(dir, dir->lookup(this->cred_, "."));
    EXPECT_EQ(root, dir->lookup(this->cred_, ".."));
    EXPECT_EQ(3, root->getattr()->size());
}

TYPED_TEST_P(FilesystemTest, Remove)
{
    auto root = this->fs_->root();
    auto link = root->symlink(this->cred_, "foo", "bar", setMode666);
    auto dir = root->mkdir(this->cred_, "baz", setMode777);
    EXPECT_THROW(root->remove(this->cred_, "baz"), std::system_error);
    root->remove(this->cred_, "foo");
    EXPECT_EQ(3, root->getattr()->size());
}

TYPED_TEST_P(FilesystemTest, Rmdir)
{
    auto root = this->fs_->root();
    auto dir = root->mkdir(this->cred_, "foo", setMode777);
    root->rmdir(this->cred_, "foo");
    EXPECT_EQ(2, int(root->getattr()->nlink()));
    EXPECT_EQ(2, root->getattr()->size());
    EXPECT_THROW(root->lookup(this->cred_, "foo"), std::system_error);

    root->symlink(this->cred_, "foo", "bar", setMode666);
    EXPECT_THROW(root->rmdir(this->cred_, "foo"), std::system_error);
}

TYPED_TEST_P(FilesystemTest, RenameFile)
{
    auto root = this->fs_->root();
    auto file = root->mkfifo(this->cred_, "foo", setMode666);

    // Simple rename within root directory
    root->rename(this->cred_, "bar", root, "foo");
    EXPECT_THROW(root->lookup(this->cred_, "foo"), std::system_error);
    EXPECT_EQ(file, root->lookup(this->cred_, "bar"));
    EXPECT_EQ(3, root->getattr()->size());

    // Rename to a different directory
    auto dir = root->mkdir(this->cred_, "foo", setMode777);
    EXPECT_EQ(4, root->getattr()->size());
    dir->rename(this->cred_, "bar", root, "bar");
    EXPECT_THROW(root->lookup(this->cred_, "bar"), std::system_error);
    EXPECT_EQ(file, dir->lookup(this->cred_, "bar"));
    EXPECT_EQ(3, root->getattr()->size());
    EXPECT_EQ(3, dir->getattr()->size());

    // Renaming to an existing name should delete the target first ...
    auto file2 = dir->mkfifo(this->cred_, "foo", setMode666);
    EXPECT_EQ(4, dir->getattr()->size());
    dir->rename(this->cred_, "foo", dir, "bar");
    EXPECT_EQ(file, dir->lookup(this->cred_, "foo"));
    EXPECT_EQ(3, dir->getattr()->size());

    // ... unless the target is a non-empty directory
    dir->mkdir(this->cred_, "bar", setMode777)
        ->mkfifo(this->cred_, "aaa", setMode666);
    EXPECT_EQ(4, dir->getattr()->size());
    EXPECT_THROW(
        dir->rename(this->cred_, "bar", dir, "foo"), std::system_error);

    // Check that the rename succeeds when the target is empty and verify
    // the size and link count
    dir->lookup(this->cred_, "bar")->remove(this->cred_, "aaa");
    EXPECT_EQ(3, dir->getattr()->nlink());
    dir->rename(this->cred_, "bar", dir, "foo");
    EXPECT_EQ(2, dir->getattr()->nlink());
    EXPECT_EQ(3, dir->getattr()->size());
}

TYPED_TEST_P(FilesystemTest, RenameDir)
{
    auto root = this->fs_->root();
    auto dir = root->mkdir(this->cred_, "foo", setMode777);
    EXPECT_EQ(3, root->getattr()->nlink());

    // Simple rename within root directory
    root->rename(this->cred_, "bar", root, "foo");
    EXPECT_THROW(root->lookup(this->cred_, "foo"), std::system_error);
    EXPECT_EQ(dir, root->lookup(this->cred_, "bar"));
    EXPECT_EQ(3, root->getattr()->nlink());

    // Rename to a different directory
    auto dir2 = root->mkdir(this->cred_, "foo", setMode777);
    EXPECT_EQ(4, root->getattr()->nlink());
    EXPECT_EQ(2, dir->getattr()->nlink());
    dir->rename(this->cred_, "foo", root, "foo");
    EXPECT_EQ(3, root->getattr()->nlink());
    EXPECT_EQ(3, dir->getattr()->nlink());
    EXPECT_THROW(root->lookup(this->cred_, "foo"), std::system_error);
    EXPECT_EQ(dir2, dir->lookup(this->cred_, "foo"));
    EXPECT_EQ(dir, dir2->lookup(this->cred_, ".."));
}

TYPED_TEST_P(FilesystemTest, Link)
{
    auto root = this->fs_->root();
    auto a = root->mkdir(this->cred_, "a", setMode777);
    auto b = root->mkfifo(this->cred_, "b", setMode777);
    EXPECT_THROW(root->link(this->cred_, "aa", a), std::system_error);
    root->link(this->cred_, "bb", b);
    EXPECT_EQ(b, root->lookup(this->cred_, "bb"));
}

TYPED_TEST_P(FilesystemTest, Readdir)
{
    auto root = this->fs_->root();
    root->mkdir(this->cred_, "foo", setMode777);
    root->mkdir(this->cred_, "bar", setMode777);
    root->mkdir(this->cred_, "baz", setMode777);
    std::set<std::string> names;
    for (auto iter = root->readdir(this->cred_, 0);
        iter->valid(); iter->next()) {
        names.insert(iter->name());
    }
    EXPECT_EQ(5, names.size());
    EXPECT_NE(names.end(), names.find("."));
    EXPECT_NE(names.end(), names.find(".."));
    EXPECT_NE(names.end(), names.find("foo"));
    EXPECT_NE(names.end(), names.find("bar"));
    EXPECT_NE(names.end(), names.find("baz"));
}

TYPED_TEST_P(FilesystemTest, DirectorySetgid)
{
    filesys::Credential cred1(99, 99, {});
    filesys::Credential cred2(100, 100, {});
    auto root = this->fs_->root();
    EXPECT_EQ(0, root->getattr()->mode() & filesys::ModeFlags::SETGID);
    this->setCred(cred1);
    auto foo = root->mkdir(
        cred1, "foo", [&](auto attr){
            attr->setMode(02777);
        });
    EXPECT_EQ(99, foo->getattr()->gid());
    this->setCred(cred2);
    auto bar = foo->mkdir(
        cred2, "bar", [](auto attr){ attr->setMode(0755); });
    EXPECT_EQ(99, bar->getattr()->gid());
}

TYPED_TEST_P(FilesystemTest, DirectoryPerms)
{
    auto root = this->fs_->root();
    auto dir = root->mkdir(
        this->cred_, "foo", [](auto attr){ attr->setMode(0500); });

    // Create should fale: directory is not writable
    EXPECT_THROW(
        dir->mkfifo(this->cred_, "a", setMode666), std::system_error);

    // Create succeeds after adding write permission
    dir->setattr(this->cred_, setMode666);
    dir->mkfifo(this->cred_, "a", setMode666);

    // Lookup fails: directory is not executable
    EXPECT_THROW(dir->lookup(this->cred_, "a"), std::system_error);

    // Readdir should succeed since the directory is readable
    std::set<std::string> names;
    for (auto iter = dir->readdir(this->cred_, 0);
        iter->valid(); iter->next()) {
        names.insert(iter->name());
    }
    EXPECT_NE(names.end(), names.find("a"));

    // Lookup succeeds after adding execute permission
    dir->setattr(this->cred_, setMode777);
    dir->lookup(this->cred_, "a");
}

TYPED_TEST_P(FilesystemTest, DirectorySticky)
{
    filesys::Credential cred1(99, 99, {});
    filesys::Credential cred2(100, 100, {});

    auto root = this->fs_->root();
    auto tmp = root->mkdir(
        this->cred_, "tmp", [](auto attr){ attr->setMode(01777); });
    auto tmp2 = root->mkdir(
        this->cred_, "tmp2", [](auto attr){ attr->setMode(01777); });

    this->setCred(cred1);
    tmp->mkfifo(cred1, "a", setMode666);
    this->setCred(cred2);
    EXPECT_THROW(tmp->remove(cred2, "a"), std::system_error);

    this->setCred(cred1);
    tmp->mkdir(cred1, "b", setMode666);
    this->setCred(cred2);
    EXPECT_THROW(tmp->rmdir(cred2, "b"), std::system_error);

    // Rename fails: source directory is sticky
    EXPECT_THROW(tmp->rename(cred2, "bb", tmp, "b"), std::system_error);

    // Rename fails: target directory has existing entry but is sticky
    this->setCred(cred1);
    tmp2->mkdir(cred1, "b", setMode666);
    this->setCred(cred2);
    EXPECT_THROW(tmp2->rename(cred2, "b", tmp, "b"), std::system_error);

    // Rename succeeds with cred1
    this->setCred(cred1);
    tmp2->rename(cred1, "b", tmp, "b");
}

REGISTER_TYPED_TEST_CASE_P(
    FilesystemTest,
    Init,
    Access,
    Open,
    ReadWrite,
    Truncate,
    Mtime,
    Atime,
    Symlink,
    Mkfifo,
    Mkdir,
    Remove,
    Rmdir,
    RenameFile,
    RenameDir,
    Link,
    Readdir,
    DirectorySetgid,
    DirectoryPerms,
    DirectorySticky
);
