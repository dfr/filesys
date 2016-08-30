Implementation Notes
====================

The goal is to have a reliable replicated key/value database which
will be used as backing store for a distributed filesystem. We use the
Paxos algorithm
(http://research.microsoft.com/en-us/um/people/lamport/pubs/paxos-simple.pdf)
to implement a replicated log of transactions. Each transaction has a
unique transaction number and contains a set of operations to be
applied to a key/value database.

A typical installation has multiple nodes (at least three), each of
which maintains a copy of the key/value data store and applies
transactions from the replicated log. One distinguished node at a time
is the 'leader' and this node is typically the only node which creates
new transaction log entries.

By definition, the leader is the node which successfully adds new
transactions to the log using the Paxos algorithm - all nodes
implicitly grant a leadership 'lease' to the last node which created a
new log entry which lasts a fixed period of time (the 'Leadership
Lease Time'). In the absence of new transactions during a lease, the
leader will create empty transactions before the end of the lease to
reduce the likelihood of leadership turnover. If a follower node has
not seen a new transaction before the end of the current lease it will
attempt to create one and if successful it becomes the new leader.

We assume that the underlying key/value store provides some kind of
consistency guarantee - this is true of the current choice of backing
store, RocksDB. We store transaction log entries in the DB in a
separate column family to keep them available if a recovering node
needs to replay the transaction log. Regular database snapshots are
taken which can be used to improve the efficiency of building a new
replica. When a snapshot is taken, the local log can be truncated for
effiency.

To build a new replica, we copy a database snapshot from one of the
other nodes, probably using NFS to handle the bulk data transfer. This
snapshot is restored into an empty local database which provides a
base state. The new replica learns the current transaction number by
observing the Paxos protocol. Any transactions which were not
contained in the snapshot can be discovered using the Paxos protocol
to retrieve log entries from the other nodes.
