/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

/*
 * An encoding for key/value database transactions
 */

enum OpType {
    OP_PUT = 0,
    OP_REMOVE = 1,
};

struct PutOp {
    string ns<>;
    opaque key<>;
    opaque value<>;
};

struct RemoveOp {
    string ns<>;
    opaque key<>;
};

union Operation switch (OpType op) {
case OP_PUT:
    PutOp put;
case OP_REMOVE:
    RemoveOp remove;
};

struct Transaction {
    Operation ops<>;
};
