// -*- c++ -*-
#pragma once

namespace nfsd {

class ThreadPool
{
public:
    ThreadPool(int workerCount)
        : pending_(0)
    {
        for (int i = 0; i < workerCount; i++)
            workers_.emplace_back(this, i);
    }

    ~ThreadPool()
    {
        assert(pending_ == 0);
    }

    void addService(
        uint32_t prog, uint32_t vers,
        std::shared_ptr<oncrpc::ServiceRegistry> svcreg,
        oncrpc::Service svc)
    {
        using std::placeholders::_1;
        svcreg->add(prog, vers, std::bind(&ThreadPool::dispatch, this, _1));
        services_[std::make_pair(prog, vers)] = std::move(svc);
    }

    void dispatch(oncrpc::CallContext&& ctx)
    {
        auto p = services_.find(std::make_pair(ctx.prog(), ctx.vers()));
        if (p == services_.end()) {
            // This should not happen - the available services are all
            // registered with the main ServiceRegistry instance
            ctx.programUnavailable();
        }
        auto svc = p->second;

        // If we have no worker threads, just dispatch in this thread.
        if (workers_.size() == 0) {
            svc(std::move(ctx));
            return;
        }

        pending_ += ctx.size();
        VLOG(3) << "xid: " << ctx.msg().xid
                << ": dispatching to worker " << nextWorker_
                << ", pending: " << pending_ << " bytes";
        ctx.setService(p->second);
        workers_[nextWorker_].add(std::move(ctx));
        nextWorker_++;
        if (nextWorker_ == workers_.size())
            nextWorker_ = 0;
    }

    struct Worker
    {
        Worker(ThreadPool* pool, int id)
            : pool_(pool),
              id_(id)
        {
            thread_ = std::thread(
                [this]() {
                    std::unique_lock<std::mutex> lock(mutex_);
                    while (!stopping_) {
                        VLOG_EVERY_N(2, 100) << "worker " << id_
                                             << ": queue size "
                                             << work_.size();
                        if (work_.size() > 0) {
                            auto ctx = std::move(work_.front());
                            work_.pop_front();
                            lock.unlock();
                            //std::this_thread::sleep_for(10ms);
                            VLOG_EVERY_N(2, 100) << "worker " << id_
                                                 << ": xid: " << ctx.msg().xid
                                                 << ": running";
                            ctx();
                            pool_->pending_ -= ctx.size();
                            lock.lock();
                        }
                        if (work_.size() == 0)
                            cv_.wait(lock);
                    }
                });
        }

        ~Worker()
        {
            stopping_ = true;
            cv_.notify_one();
            thread_.join();
        }

        void add(oncrpc::CallContext&& ctx)
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_.emplace_back(std::move(ctx));
            cv_.notify_one();
        }

        std::thread thread_;
        std::mutex mutex_;
        std::condition_variable cv_;
        std::deque<oncrpc::CallContext> work_;
        bool stopping_ = false;
        ThreadPool* pool_;
        int id_;
    };

    std::deque<Worker> workers_;
    int nextWorker_ = 0;
    std::atomic_int pending_;
    std::unordered_map<std::pair<uint32_t, uint32_t>,
        oncrpc::Service> services_;
};

}