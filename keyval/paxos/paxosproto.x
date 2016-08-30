/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

/*
 * RPC protocol for paxos distributed consensus
 */

enum ReplicaStatus {
    STATUS_DEAD       = 0,
    STATUS_HEALTHY    = 1,
    STATUS_RECOVERING = 2,
    STATUS_UNKNOWN    = 3
};

/*
 * A paxos round number. This has two parts - a generation number
 * which is incremented for each round of the protocol and an id
 * number which uniquely identifies the coordinator for the round
 * (this ensures that two conflicting coordinators will not choose
 * the same round number.
 */
struct PaxosRound {
    int gen;                    /* generation number */
    UUID id;                    /* identifier of coordinator */
};

/*
 * A pending client command
 */
typedef opaque PaxosCommand<>;

struct IDENTITYargs {
    UUID uuid;                  /* identity of sender */
    ReplicaStatus status;       /* sender's current status */
    hyper instance;             /* last instance sender participated in */
};

struct PREPAREargs {
    UUID uuid;                  /* identity of sender */
    hyper instance;             /* instance number to prepare */
    PaxosRound i;               /* paxos round number */
};

struct PROMISEargs {
    UUID uuid;                  /* identity of sender */
    hyper instance;             /* instance number to promise */
    PaxosRound i;               /* paxos round number */
    PaxosRound vrnd;            /* highest round which sender has voted in */
    PaxosCommand vval;          /* value sender voted for in round vrnd */
};

struct ACCEPTargs {
    UUID uuid;                  /* identity of sender */
    hyper instance;             /* instance number to accept */
    PaxosRound i;               /* paxos round number */
    PaxosCommand v;             /* value being accepted */
};

struct NACKargs {
    UUID uuid;                  /* identity of sender */
    hyper instance;             /* instance number */
    PaxosRound i;               /* paxos round number */
};

/*
 * The Paxos network protocol.
 *
 * An acceptor a maintains only the following data:
 *
 *   rnd[a]  The highest-numbered round in which a has participated,
 *           initially 0. (Since 0 is not a round number, rnd[a] = 0
 *           means that a has not participated in any round.)
 *
 *   vrnd[a] The highest-numbered round in which a has cast a vote,
 *           initially 0. (Hence, vrnd[a] <= rnd[a] is always true.) 
 *
 *   vval[a] The value a voted to accept in round vrnd[a]; its initial
 *           value (when vrnd[a] = 0) is irrelevant.
 *
 * Each coordinator c maintains the following data:
 *           
 *   crnd[c] The highest-numbered round that c has begun, initially 0.
 *           
 *   cval[c] The value that c has picked for round crnd[c], or the
 *           special value none if c has not yet picked a value for
 *           that round. Its initial value is irrelevant.
 */
program PAXOS {
    version PAXOSVER {
        /*
         * Null procedure.
         */
        void PAXOSPROC_NULL(void) = 0;
    
        /*
         * Weak leadership election. Replicas send IDENTITY messages
         * periodically to indicate their status.
         */
        oneway PAXOSPROC_IDENTITY(IDENTITYargs) = 1;

        /*
         * Paxos phase 1a
         *
         * If crnd[c] < i, then c starts round i by setting crnd[c] to i,
         * setting cval[c] to none, and sending a message to each acceptor
         * a requesting that a participate in round i.
         */
        oneway PAXOSPROC_PREPARE(PREPAREargs) = 2;
        
        /*
         * Paxos phase 1b
         *
         * If an acceptor a receives a request to participate in round i
         * and i > rnd[a], then a sets rnd[a] to i and sends coordinator c
         * a message containing the round number i and the current values
         * of vrnd[a] and vval[a].
         *
         * If i != rnd[a] (so a has begun round i or a higher-numbered
         * round), then a ignores the request.
         *
         * Note: as an optimization, we modify this so that the acceptor
         * sends a nack message with the highest round that it has
         * promised so far.
         */
        oneway PAXOSPROC_PROMISE(PROMISEargs) = 3;

        /*
         * Paxos phase 2a
         *
         * If crnd[c] = i (so c has not begun a higher-numbered round),
         * cval[c] = none (so c has not yet performed phase 2a for this
         * round), and c has received phase 1b messages for round i from a
         * majority of the acceptors; then by a rule described below, c
         * uses the contents of those messages to pick a value v, sets
         * cval[c] to v, and sends a message to the acceptors requesting
         * that they vote in round i to accept v.
         */
        oneway PAXOSPROC_ACCEPT(ACCEPTargs) = 4;

        /*
         * Paxos phase 2b
         *
         * If an acceptor a receives a request to vote in round i to
         * accept a value v, and i >= rnd[a] and vrnd[a] != i; then a
         * votes in round i to accept v, sets vrnd[a] and rnd[a] to i,
         * sets vval[a] to v, and sends a message to all learners
         * announcing its round i vote.
         *
         * If i < rnd[a] or vrnd[a] != i (so a has begun a higher-numbered
         * round or already voted in this round), then a ignores the
         * request.
         *
         * Note: again as an optimization, we modify this so that the
         * acceptor sends a nack message with the highest round that it
         * has promised so far.
         */
        oneway PAXOSPROC_ACCEPTED(ACCEPTargs) = 5;

        /*
         * The nack message is a response to prepare or accept with a
         * round which is older than the last one we responded to.
         */
        oneway PAXOSPROC_NACK(NACKargs) = 6;
    } = 1;

} = 0x20160816;
