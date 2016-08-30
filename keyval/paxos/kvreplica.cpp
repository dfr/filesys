/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <chrono>
#include <iomanip>
#include <random>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "paxos.h"
#include "keyval/paxos/kvproto.h"

using namespace keyval;
using namespace keyval::paxos;

class KVNamespace: public keyval::Namespace
{
public:
    KVNamespace(KVReplica* replica, const std::string& name)
        : name_(name),
          ns_(replica->db()->getNamespace(name))
    {
    }

    std::unique_ptr<Iterator> iterator() override
    {
        return ns_->iterator();
    }

    std::unique_ptr<Iterator> iterator(std::shared_ptr<Buffer> key) override
    {
        return ns_->iterator(key);
    }

    std::shared_ptr<Buffer> get(std::shared_ptr<Buffer> key) override
    {
        return ns_->get(key);
    }

    std::uint64_t spaceUsed(
        std::shared_ptr<Buffer> start, std::shared_ptr<Buffer> end) override
    {
        return ns_->spaceUsed(start, end);
    }

    auto& name() const { return name_; }

private:
    std::string name_;
    std::shared_ptr<keyval::Namespace> ns_;
};

class KVTransaction: public keyval::Transaction
{
public:
    void put(
        std::shared_ptr<Namespace> ns,
        std::shared_ptr<Buffer> key, std::shared_ptr<Buffer> val) override
    {
        auto kvns = reinterpret_cast<KVNamespace*>(ns.get());
        trans_.ops.emplace_back(
            Operation(
                OP_PUT,
                PutOp{kvns->name(), toVector(key), toVector(val)}));
    }

    void remove(
        std::shared_ptr<Namespace> ns, std::shared_ptr<Buffer> key) override
    {
        auto kvns = reinterpret_cast<KVNamespace*>(ns.get());
        trans_.ops.emplace_back(
            Operation(
                OP_REMOVE,
                RemoveOp{kvns->name(), toVector(key)}));
    }

    std::vector<uint8_t> toVector(std::shared_ptr<Buffer> buf)
    {
        std::vector<uint8_t> res(buf->size());
        std::copy_n(buf->data(), buf->size(), res.data());
        return res;
    }

    std::vector<uint8_t> encode() const
    {
        std::vector<uint8_t> res(oncrpc::XdrSizeof(trans_));
        oncrpc::XdrMemory xm(res.data(), res.size());
        xdr(trans_, static_cast<oncrpc::XdrSink*>(&xm));
        return res;
    }

private:
    keyval::paxos::Transaction trans_;

};

KVReplica::KVReplica(
    std::shared_ptr<IPaxos1> proto,
    std::shared_ptr<util::Clock> clock,
    std::shared_ptr<oncrpc::TimeoutManager> tman,
    std::shared_ptr<Database> db)
    : Replica(proto, clock, tman, db)
{
}

KVReplica::KVReplica(
    const std::string& replicaAddress,
    std::shared_ptr<util::Clock> clock,
    std::shared_ptr<oncrpc::SocketManager> sockman,
    std::shared_ptr<Database> db)
    : Replica(replicaAddress, clock, sockman, db)
{
}

void KVReplica::apply(
    std::int64_t instance, const std::vector<uint8_t>& command)
{
    keyval::paxos::Transaction kvtrans;
    oncrpc::XdrMemory xm(command.data(), command.size());
    xdr(kvtrans, static_cast<oncrpc::XdrSource*>(&xm));

    auto trans = db()->beginTransaction();
    for (auto& op: kvtrans.ops) {
        switch (op.op) {
        case OP_PUT:
            VLOG(1) << this << ": put "
                    << op.put().ns
                    << ", " << op.put().key
                    << ", " << op.put().value;
            trans->put(
                db()->getNamespace(op.put().ns),
                toBuffer(op.put().key),
                toBuffer(op.put().value));
            break;
        case OP_REMOVE:
            VLOG(1) << this << ": remove "
                    << op.remove().ns
                    << ", " << op.remove().key;
            trans->remove(
                db()->getNamespace(op.put().ns),
                toBuffer(op.put().key));
            break;
        }
    }

    // Write the instance number in the same transaction
    saveInstance(instance, trans.get());

    db()->commit(std::move(trans));
}

void KVReplica::leaderChanged()
{
    LOG(INFO) << this << ": leaderChanged: " << isLeader_;
    for (auto& cb: masterChangeCallbacks_)
        cb(isLeader_);
}

std::shared_ptr<Namespace> KVReplica::getNamespace(const std::string& name)
{
    return std::make_shared<KVNamespace>(this, name);
}

std::unique_ptr<keyval::Transaction> KVReplica::beginTransaction()
{
    return std::make_unique<KVTransaction>();
}

void KVReplica::commit(std::unique_ptr<keyval::Transaction>&& transaction)
{
    VLOG(1) << this << ": committing transaction";
    auto p = reinterpret_cast<KVTransaction*>(transaction.get());
    auto pt = execute(p->encode());
    transaction.reset();
    VLOG(1) << this << ": waiting for completion";
    pt->wait();
}

void KVReplica::flush()
{
}

void KVReplica::onMasterChange(std::function<void(bool)> cb)
{
    masterChangeCallbacks_.push_back(cb);
}
