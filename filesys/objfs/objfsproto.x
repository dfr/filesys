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
    unsigned hyper nextId;  /* next fileid to use */
    unsigned hyper fileCount; /* number of files in this filesystem */
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
 * Per-file metadata
 */
struct ObjFileMeta
{
    int vers;			/* = 1 */
    unsigned hyper fileid;	/* unique file identifier */
    unsigned blockSize;		/* file block size */
    PosixAttr attr;		/* posix-style file attributes */
    opaque extra<>;
};

struct DirectoryEntry
{
    unsigned hyper fileid;
};
