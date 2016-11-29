/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
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
