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

#pragma once

#include <chrono>

namespace util {

/// A simple wrapper for system_clock::now which we can mock for testing
class Clock
{
public:
    typedef std::chrono::system_clock::time_point time_point;
    typedef std::chrono::system_clock::duration duration;
    virtual ~Clock() {}
    virtual time_point now() = 0;
};

/// Clock implementation using system_clock
class SystemClock: public Clock
{
public:
    virtual time_point now()
    {
        return std::chrono::system_clock::now();
    }
};

/// Fake clock which can be used for time-related unit tests
class MockClock: public Clock
{
public:
    MockClock()
        : now_(std::chrono::system_clock::now())
    {
    }

    auto lock()
    {
        return std::unique_lock<std::mutex>(mutex_);
    }

    time_point now() override { return now_; }

    template <typename Dur>
    MockClock& operator+=(Dur dur)
    {
        auto lk = lock();
        assert(dur.count() >= 0);
        now_ += dur;
        return *this;
    }

private:
    std::mutex mutex_;
    time_point now_;
};

}
