/* -*- c -*- */

/*
 * The time between device status updates in seconds. Each data
 * server transmits a status update roughly once each heartbeat period
 * and we use this to detect failed servers.
 */
const DISTFS_HEARTBEAT = 30;

enum distfsstat {
    DISTFS_OK		= 0,
    DISTFSERR_PERM	= 1,
    DISTFSERR_NOENT	= 2,
    DISTFSERR_IO	= 5,
};

const DISTFS_VERIFIER_SIZE = 8;
const DISTFS_OPAQUE_LIMIT = 1024;
const NFS4_FHSIZE = 128;

typedef opaque distfs_verf[DISTFS_VERIFIER_SIZE];
typedef opaque distfs_fh<NFS4_FHSIZE>;

/*
 * Device owner - the device sends this to the metadata
 * server. The ds_ownerid field should uniquely identify the data
 * server across reboots and the do_verifier field should change each
 * time the device restarts.
 */
struct distfs_owner {
    distfs_verf do_verifier;
    opaque do_ownerid<DISTFS_OPAQUE_LIMIT>;
};

/*
 * The metadata server assigns an ID to each unique device and
 * uses it to track which device's have copies of any given piece
 * of data.
 */
typedef uint64_t devid;

/*
 * Each piece is replicated to multiple devices - we keep track
 * of each replica using the ID for the device and a unique piece
 * index on that server
 */
struct PieceIndex
{
    devid device;
    uint64_t index;
};

/*
 * Each piece of data has an entry in the data namespace with the key
 * made up from the fileid and piece offset and the data giving the
 * piece locations.
 */
typedef PieceIndex PieceLocation<>;

typedef string uaddr<>;

/*
 * This is sent by a device to announce its status and contact
 * addresses. We also store this in the MDS database.
 */
struct DeviceStatus
{
    /*
     * A unique identifier for the device
     */
    distfs_owner owner;

    /*
     * Address to use for accessing data
     */
    uaddr uaddrs<>;
};

/*
 * Details of available storage on a device. Sizes are in bytes.
 */
struct StorageStatus
{
    uint64_t totalSpace;
    uint64_t freeSpace;
    uint64_t availSpace;
};

struct STATUSargs {
    DeviceStatus device;
    StorageStatus storage;
};

/*
 * The metadata server implements this program to allow devices
 * to register themselves as they start up
 */
program DISTFS_MDS {
    version MDS_V1 {
	void MDS_NULL(void) = 0;

	/*
	 * Called by devices to announce their existence and
	 * status
	 */
	oneway MDS_STATUS(STATUSargs) = 1;
    } = 1;
} = 1234;			/* XXX */

struct FINDPIECEargs {
    uint64_t fileid;
    uint64_t offset;
    uint32_t size;
};

struct FINDPIECEresok {
    distfs_fh object;
};

union FINDPIECEres switch (distfsstat status) {
case DISTFS_OK:
    FINDPIECEresok resok;
default:
    void;
};

struct CREATEPIECEargs {
    uint64_t fileid;
    uint64_t offset;
    uint32_t size;
};

struct CREATEPIECEresok {
    distfs_fh object;
    StorageStatus storage;
};

union CREATEPIECEres switch (distfsstat status) {
case DISTFS_OK:
    CREATEPIECEresok resok;
default:
    void;
};

struct REMOVEPIECEargs {
    uint64_t fileid;
    uint64_t offset;
    uint32_t size;
};

struct REMOVEPIECEres {
    distfsstat status;
};

struct TRUNCATEPIECEargs {
    uint64_t fileid;
    uint64_t offset;
    uint32_t size;
    uint64_t truncateOffset;
};

struct TRUNCATEPIECEres {
    distfsstat status;
};

/*
 * The devices implement this program to allow the meta data server
 * to manage the device's piece collection
 */
program DISTFS_DS {
    version DS_V1 {
	void DS_NULL(void) = 0;

	/*
	 * Return a filehandle which can be used to access a data
	 * piece of the file identified by fileid at the given offset.
	 */
	FINDPIECEres DS_FIND_PIECE(FINDPIECEargs) = 1;

	/*
	 * Create a data piece for the given fileid and offset,
	 * returning a filehandle which can be used to access the new
	 * piece.
	 */
	CREATEPIECEres DS_CREATE_PIECE(CREATEPIECEargs) = 2;

	/*
	 * Delete a data piece
	 */
	REMOVEPIECEres DS_REMOVE_PIECE(REMOVEPIECEargs) = 3;
    } = 1;
} = 1235;			/* XXX */
