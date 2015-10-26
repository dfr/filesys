/*
 * Per-filesystem metadata
 */
struct UUID
{
    unsigned int data[4];
};

struct ObjFilesystemMeta
{
    int vers;               /* = 1 */
    UUID fsid;              /* unique identifier for this filesystem */
    unsigned blockSize;
    unsigned hyper nextId;  /* next fileid to use */
};

/*
 * Posix file attributes
 */
enum PosixType {
	PT_REG = 1,	/* regular file */
	PT_DIR = 2,	/* directory */
	PT_BLK = 3,	/* block special */
	PT_CHR = 4,	/* character special */
	PT_LNK = 5,	/* symbolic link */
	PT_SOCK = 6, /* unix domain sockets */
	PT_FIFO = 7 /* named pipe */
};

struct PosixAttr {
	PosixType type;       /* file type */
	unsigned mode;        /* protection mode bits */
	unsigned nlink;       /* # hard links */
	unsigned uid;         /* owner user id */
	unsigned gid;         /* owner group id */
	unsigned hyper size;  /* file size in bytes */
	unsigned hyper atime; /* time of last access */
	unsigned hyper mtime; /* time of last modification */
	unsigned hyper ctime; /* time of last change */
    unsigned hyper birthtime; /* birth time */
};

/*
 * File data location
 */
enum DataLocationType {
    LOC_EMBEDDED = 0,   /* embedded in metadata */
    LOC_FILE = 1,       /* local file */
    LOC_DB = 2,         /* local database */
    LOC_NFS = 3,        /* nfs data server */
};

/*
 * Data embedded in file metadata
 */
struct EmbeddedDataLocation {
    opaque data<>;
};

/*
 * Data in local posix file
 */
struct FileDataLocation {
    string filename<>;
};

/*
 * Data in local object database
 */
 struct DBDataLocation
 {
     unsigned blockSize;
 };

/*
 * Data on remote NFS file server
 */
struct NfsDataLocation {
    string uaddr<>;
    opaque handle<>;
};

union DataLocation switch (DataLocationType type) {
case LOC_EMBEDDED:
    EmbeddedDataLocation embedded;
case LOC_FILE:
    FileDataLocation file;
case LOC_DB:
    DBDataLocation db;
case LOC_NFS:
    NfsDataLocation nfs;
};

/*
 * Per-file metadata
 */
struct ObjFileMeta
{
    int vers;               /* = 1 */
    unsigned hyper fileid;
    PosixAttr attr;
    DataLocation location;
};

struct DirectoryEntry
{
    unsigned hyper fileid;
};
