/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

namespace keyval {
namespace paxos {

/// An implementation of IPaxos1 which copies each message to a set of
/// receivers asynchronously. When adding callbacks for each replica,
/// we lock the clock to ensure that MyTimeoutManager cannot run until
/// we have added all the callbacks.
struct Reflector: public IPaxos1
{
    Reflector(
        std::shared_ptr<util::MockClock> clock,
        std::shared_ptr<oncrpc::TimeoutManager> tman)
        : clock_(clock),
          tman_(tman)
    {
    }

    ~Reflector()
    {
        // Make sure the replicas are destroyed before the timeout
        // manager
        replicas_.clear();
    }

    // IPaxos1 overrides - we copy each message to all the replicas
    // asynchronously
    void null() override
    {
    }
    void identity(const IDENTITYargs& args) override
    {
        auto lk = clock_->lock();
        auto now = clock_->now();
        for (auto& entry: replicas_) {
            if (!entry.enabled)
                continue;
            auto replica = entry.proto;
            tman_->add(
                now, [replica, args]() {
                    replica->identity(args);
                });
        }
    }
    void prepare(const PREPAREargs& args) override
    {
        auto lk = clock_->lock();
        auto now = clock_->now();
        for (auto& entry: replicas_) {
            if (!entry.enabled)
                continue;
            auto replica = entry.proto;
            tman_->add(
                now, [replica, args]() {
                    replica->prepare(args);
                });
        }
    }
    void promise(const PROMISEargs& args) override
    {
        auto lk = clock_->lock();
        auto now = clock_->now();
        for (auto& entry: replicas_) {
            if (!entry.enabled)
                continue;
            auto replica = entry.proto;
            tman_->add(
                now, [replica, args]() {
                    replica->promise(args);
                });
        }
    }
    void accept(const ACCEPTargs& args) override
    {
        auto lk = clock_->lock();
        auto now = clock_->now();
        for (auto& entry: replicas_) {
            if (!entry.enabled)
                continue;
            auto replica = entry.proto;
            tman_->add(
                now, [replica, args]() {
                    replica->accept(args);
                });
        }
    }
    void accepted(const ACCEPTargs& args) override
    {
        auto lk = clock_->lock();
        auto now = clock_->now();
        for (auto& entry: replicas_) {
            if (!entry.enabled)
                continue;
            auto replica = entry.proto;
            tman_->add(
                now, [replica, args]() {
                    replica->accepted(args);
                });
        }
    }
    void nack(const NACKargs& args) override
    {
        auto lk = clock_->lock();
        auto now = clock_->now();
        for (auto& entry: replicas_) {
            if (!entry.enabled)
                continue;
            auto replica = entry.proto;
            tman_->add(
                now, [replica, args]() {
                    replica->nack(args);
                });
        }
    }

    void add(std::shared_ptr<IPaxos1> replica)
    {
        replicas_.push_back({replica, true});
    }

    void enable(int index)
    {
        replicas_[index].enabled = true;
    }

    void disable(int index)
    {
        replicas_[index].enabled = false;
    }

    void set(int index, std::shared_ptr<IPaxos1> replica)
    {
        replicas_[index] = {replica, true};
    }

    struct Entry
    {
        std::shared_ptr<IPaxos1> proto;
        bool enabled;
    };

    std::shared_ptr<util::MockClock> clock_;
    std::shared_ptr<oncrpc::TimeoutManager> tman_;
    std::vector<Entry> replicas_;
};

}
}
