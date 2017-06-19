/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
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
