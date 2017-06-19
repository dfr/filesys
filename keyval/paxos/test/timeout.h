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

#include <rpc++/timeout.h>

namespace keyval {
namespace paxos {

/// An implementation of TimeoutManager which executes the 'timeouts'
/// immediately on a thread
struct MyTimeoutManager: public oncrpc::TimeoutManager
{
    MyTimeoutManager(std::shared_ptr<util::MockClock> clock)
        : clock_(clock)
    {
    }

    // TimeoutManager overrides
    task_type add(
        clock_type::time_point when, std::function<void()> what) override
    {
        auto res = TimeoutManager::add(when, what);
        std::unique_lock<std::mutex> lk(mutex_);
        VLOG(2) << "adding at " << when.time_since_epoch().count()
                << ", min " << queue_.front().when.time_since_epoch().count()
                << ", " << queue_.size() << " pending";
        cv_.notify_all();
        return res;
    }

    void start()
    {
        thread_ = std::thread([this]() { run(); });
    }

    void stop()
    {
        std::unique_lock<std::mutex> lk(mutex_);
        stopping_ = true;
        cv_.notify_all();
        lk.unlock();
        thread_.join();
    }

    void run()
    {
        std::unique_lock<std::mutex> lk(mutex_);
        while (!stopping_) {
            if (queue_.size() > 0) {
                auto when = queue_.front().when;
                VLOG(2) << "executing at " << when.time_since_epoch().count()
                        << ", " << queue_.size() << " pending";
                // We must drop the lock before touching the clock to
                // avoid a lock order violation with Reflector which
                // has the clock locked while adding callbacks.
                lk.unlock();
                *clock_ += (when - clock_->now());
                update(when);
                lk.lock();
            }
            else {
                cv_.wait(lk);
            }
        }
    }

    std::shared_ptr<util::MockClock> clock_;
    std::condition_variable cv_;
    bool stopping_ = false;
    std::thread thread_;
};

}
}
