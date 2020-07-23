/*******************************************************************************
 * tests/in_test.cpp
 *
 * basic insert and find test for more information see below
 *
 * Part of Project growt - https://github.com/TooBiased/growt.git
 *
 * Copyright (C) 2015-2016 Tobias Maier <t.maier@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include "tests/selection.h"

#include "utils/default_hash.hpp"
#include "utils/thread_coordination.hpp"
#include "utils/pin_thread.hpp"
#include "utils/command_line_parser.hpp"
#include "utils/output.hpp"
#include "utils/test_util.h"
#include <random>
#include <iostream>

#ifdef MALLOC_COUNT
#include "malloc_count.h"
#endif

/*
 * This Test is meant to test the tables performance on uniform random inputs.
 * 0. Creating 2n random keys
 * 1. Inserting n elements (key, index) - the index can be used for validation
 * 2. Looking for n elements - using different keys (likely not finding any)
 * 3. Looking for the n inserted elements (hopefully finding all)
 *    (correctness test using the index)
 */

const static uint64_t range = (1ull << 62) -1;
namespace otm = utils_tm::out_tm;
namespace ttm = utils_tm::thread_tm;

alignas(64) static HASHTYPE hash_table = HASHTYPE(0);
alignas(64) static uint64_t* keys;
alignas(64) static std::atomic_size_t current_block;
alignas(64) static std::atomic_size_t errors;

int generate_random(size_t n)
{
    std::uniform_int_distribution<uint64_t> dis(2,range);
    ttm::execute_blockwise_parallel(current_block, n,
        [&dis](size_t s, size_t e)
        {
            std::mt19937_64 re(s*10293903128401092ull);
            for (size_t i = s; i < e; i++)
            {
                keys[i] = dis(re);
            }
        });

    return 0;
}

template <class Hash>
void insert_key( Hash &hash, unsigned int & err, size_t i);

template<class Hash>
int fill_direct(Hash & hash, size_t start, size_t end){

    auto err = 0u;
    for(size_t i = start; i < end; i++){
        insert_key<Hash>(hash, err, i);
    }
    errors.fetch_add(err, std::memory_order_relaxed);
    return 0;
}



template <class Hash>
int fill(Hash& hash, size_t end)
{
    auto err = 0u;

    ttm::execute_parallel(current_block, end,
        [&hash, &err](size_t i)
        {
            insert_key(hash, err, i);
        });

    #ifdef DEBUG
        std::cout <<"fill error " << err << std::endl;
    #endif

    errors.fetch_add(err, std::memory_order_relaxed);
    return 0;
}

template <class Hash>
void insert_key(const Hash &hash, unsigned int & err, size_t i) {
    auto key = keys[i];
    if (! hash.insert(key, i+2).second )
    {
        // Insertion failed? Possibly already inserted.
        ++err;

    }
}

template <class Hash>
void getElement( Hash &hash, unsigned int err, int i);

template<class Hash>
int find_unsucc_range(Hash & hash, size_t start, size_t end)
{
    auto err = 0u;

    for(size_t i = start; i < end; i++){
       getElement<Hash>(hash, err, i);
    }


#ifdef DEBUG
    std::cout <<"find unsuccess error " << err << std::endl;
#endif
    errors.fetch_add(err, std::memory_order_relaxed);
    return 0;
}

template <class Hash>
inline void getElement( Hash &hash, unsigned int err, int i) {
    auto key = keys[i];

    auto data = hash.find(key);

    if (data != hash.end())
    {
        // Random key found (unexpected)
        ++err;
    }
}

template <class Hash>
int find_unsucc(Hash& hash, size_t begin, size_t end)
{
    auto err = 0u;

    ttm::execute_blockwise_parallel(current_block, end,
        [&hash, &err, begin](size_t s, size_t e)
        {
        for(size_t i = s; i < e; i++){
            auto key = keys[i];

            auto data = hash.find(key);

            if (data != hash.end())
            {
                // Random key found (unexpected)
                ++err;
            }
        }

        });

    #ifdef DEBUG
        std::cout <<"find unsuccess error " << err << std::endl;
    #endif
    errors.fetch_add(err, std::memory_order_relaxed);
    return 0;
}


template <class Hash>
int find_succ(Hash& hash, size_t end)
{
    auto err = 0u;

    ttm::execute_parallel(current_block, end,
        [&hash, &err, end](size_t i)
        {
            auto key = keys[i];

            auto data = hash.find(key);

            if (data == hash.end()) // || (*data).second != i+2)
            {
                ++err;
            }
        });
    #ifdef DEBUG
        std::cout <<"find sucessful error " << err << std::endl;
    #endif
    errors.fetch_add(err, std::memory_order_relaxed);
    return 0;
}

template <class ThreadType>
struct test_in_stages
{
    static int execute(ThreadType t, size_t n, size_t cap, size_t it)
    {
        using Handle = typename HASHTYPE::Handle;

        utils_tm::pin_to_core(t.id);
        if (ThreadType::is_main)
        {
            keys = new uint64_t[2*n];
        }

        // STAGE0 Create Random Keys
        {
            if (ThreadType::is_main) current_block.store (0);

            t.synchronized(generate_random, 2*n);
        }

        for (size_t i = 0; i < it; ++i)
        {
            // STAGE 0.1
            t.synchronized(
                [cap] (bool m) { if (m) hash_table = HASHTYPE(cap); return 0; },
                ThreadType::is_main);

            t.out << otm::width(3) << i
                  << otm::width(3) << t.p
                  << otm::width(9) << n
                  << otm::width(9) << cap;

            t.synchronize();

            Handle hash = hash_table.get_handle();
            size_t start;
            size_t end;

            // STAGE2 n Insertions
            {
                if (ThreadType::is_main) current_block.store(0);

                loop_range(t, 0, n, start, end);

               auto duration = t.synchronized(fill<Handle>,hash, n);;
                t.out << otm::width(10) << duration.second/1000000.;
            }

            // STAGE3 n Finds Unsuccessful
            {
                if (ThreadType::is_main) current_block.store(n);
                loop_range(t, n, 2*n, start, end);
                auto duration = t.synchronized(find_unsucc_range<Handle>, hash, start, end);

                t.out << otm::width(10) << duration.second/1000000.;
            }

            // STAGE4 n Finds Successful
            {
                if (ThreadType::is_main) current_block.store(0);

                auto duration = t.synchronized(find_succ<Handle>,
                                               hash, n);

                t.out << otm::width(10) << duration.second/1000000.;
                t.out << otm::width(10) << errors.load();
            }

#ifdef MALLOC_COUNT
            t.out << otm::width(14) << malloc_count_current();
#endif

            t.out << std::endl;
            if (ThreadType::is_main) errors.store(0);

            // Some Synchronization
            t.synchronize();
        }

        if (ThreadType::is_main)
        {
            delete[] keys;
        }

        return 0;
    }


};



int main(int argn, char** argc)
{
    utils_tm::command_line_parser c{argn, argc};
    size_t n   = c.int_arg("-n" , 10000000);
    size_t p   = c.int_arg("-p" , 4);
    size_t cap = c.int_arg("-c" , n);
    size_t it  = c.int_arg("-it", 5);
    if (! c.report()) return 1;

    otm::out() << otm::width(3)  << "#i"
               << otm::width(3)  << "p"
               << otm::width(9)  << "n"
               << otm::width(9)  << "cap"
               << otm::width(10) << "t_ins"
               << otm::width(10) << "t_find_-"
               << otm::width(10) << "t_find_+"
               << otm::width(7)  << "errors"
               << std::endl;

    ttm::start_threads<test_in_stages>(p, n, cap, it);
    return 0;
}
