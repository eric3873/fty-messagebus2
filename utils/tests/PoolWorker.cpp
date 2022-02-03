/*  =========================================================================
    PoolWorker.cpp - description

    Copyright (C) 2014 - 2021 Eaton

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
    =========================================================================
*/

#include <catch2/catch.hpp>
#include <fty/messagebus/utils/MsgBusPoolWorker.hpp>
#include <iostream>
#include <numeric>
#include <set>

using namespace fty::messagebus::utils;

uint64_t collatz(uint64_t i)
{
    uint64_t n;
    for (n = 0; i > 1; n++) {
        if (i % 2) {
            i = 3 * i + 1;
        } else {
            i = i / 2;
        }
    }
    return n;
}

int summation(std::vector<uint64_t> data)
{
    return std::accumulate(data.begin(), data.end(), 0);
}

TEST_CASE("Pool worker")
{
    std::cerr << " * MsgBusPoolWorker: " << std::endl;
    constexpr size_t NB_WORKERS = 16;
    constexpr size_t NB_JOBS    = 8 * 1024;

    // Job offloading test.
    {
        for (size_t nWorkers = 0; nWorkers < NB_WORKERS; nWorkers = nWorkers * 2 + 1) {
            std::cerr << "  - Array initialization with PoolWorker(" << nWorkers << "): ";

            std::vector<std::atomic_uint_fast32_t> results(NB_JOBS);
            {
                PoolWorker pool(nWorkers);
                for (size_t i = 0; i < NB_JOBS; i++) {
                    pool.offload(
                        [&results](size_t index) {
                            results[index].store(index);
                        },
                        i);
                }
            }

            for (size_t i = 0; i < NB_JOBS; i++) {
                assert(results[i].load() == i);
            }

            std::cerr << "OK" << std::endl;
        }
    }

    // Job queueing test.
    {
        std::array<uint64_t, NB_JOBS> collatzExpectedResults;
        for (size_t i = 0; i < NB_JOBS; i++) {
            collatzExpectedResults[i] = collatz(i);
        }

        for (size_t nWorkers = 0; nWorkers < NB_WORKERS; nWorkers = nWorkers * 2 + 1) {
            std::cerr << "  - Collatz sequence with PoolWorker(" << nWorkers << "): ";

            PoolWorker                                 pool(nWorkers);
            std::array<std::future<uint64_t>, NB_JOBS> futuresArray;
            for (uint64_t i = 0; i < NB_JOBS; i++) {
                futuresArray[i] = pool.queue(collatz, i);
            }

            for (size_t i = 0; i < NB_JOBS; i++) {
                assert(futuresArray[i].get() == collatzExpectedResults[i]);
            }

            std::cerr << "OK" << std::endl;
        }
    }

    // Job scheduling test.
    {
        for (size_t nWorkers = 1; nWorkers < NB_WORKERS; nWorkers = nWorkers * 2 + 1) {
            std::cerr << "  - Integer enumeration with PoolWorker(" << nWorkers << "): ";

            std::array<std::promise<uint64_t>, NB_JOBS>       promisesArray;
            std::array<std::shared_future<uint64_t>, NB_JOBS> futuresArray;
            {
                PoolWorker pool(nWorkers);
                for (uint64_t i = 0; i < NB_JOBS; i++) {
                    futuresArray[i] = std::shared_future(promisesArray[i].get_future());

                    pool.schedule(
                        [&promisesArray](uint64_t value) {
                            uint64_t next_value_1 = value * 2;
                            uint64_t next_value_2 = value * 2 + 1;

                            if (next_value_1 <= NB_JOBS) {
                                promisesArray[next_value_1 - 1].set_value(next_value_1);
                            }
                            if (next_value_2 <= NB_JOBS) {
                                promisesArray[next_value_2 - 1].set_value(next_value_2);
                            }
                        },
                        futuresArray[i]);
                }
                promisesArray[0].set_value(1);
            }

            for (size_t i = 0; i < NB_JOBS; i++) {
                auto a = futuresArray[i].get();
                assert(a == i + 1);
            }

            std::cerr << "OK" << std::endl;
        }
    }

    // Job scheduling test with apply.
    {
        std::cerr << "  - Schedule with apply: ";
        std::atomic_int result;

        {
            PoolWorker pool(1);
            auto       promise = std::promise<std::tuple<int, int>>();
            auto       future  = std::shared_future(promise.get_future());

            pool.scheduleWithApply(
                [&result](int a, int b) {
                    result = a + b;
                },
                future);

            promise.set_value({2, 3});
        }

        assert(result.load() == 5);

        std::cerr << "OK" << std::endl;
    }
}
