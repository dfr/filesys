/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
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

    time_point now() override { return now_; }

    template <typename Dur>
    MockClock& operator+=(Dur dur)
    {
        now_ += dur;
        return *this;
    }

private:
    time_point now_;
};

}
