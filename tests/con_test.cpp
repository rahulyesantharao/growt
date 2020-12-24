/*******************************************************************************
 * tests/con_test.cpp
 *
 * basic contention test for more information see below
 *
 * Part of Project growt - https://github.com/TooBiased/growt.git
 *
 * Copyright (C) 2015-2016 Tobias Maier <t.maier@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include "tests/selection.h"

#include "data-structures/returnelement.h"

#include "wrapper/robinhood_wrapper.h"
#include "utils/default_hash.hpp"
#include "utils/zipf_keygen.hpp"
#include "utils/thread_coordination.hpp"
#include "utils/pin_thread.hpp"
#include "utils/command_line_parser.hpp"
#include "utils/output.hpp"
#include "utils/file_processing/dist_reader.h"


#include "example/update_fcts.h"
#include "unordered_map"
#include <algorithm>
#include <random>



/*
 * This Test is meant to test the tables performance on uniform random inputs.
 * 0. Creating n random keys with zipf distribution
 * 1. Inserting n elements [1..n] (key, key)
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
alignas(64) static utils_tm::zipf_generator zipf_gen;



int generate_random(size_t n, dist_reader & reader)
{
    std::uniform_real_distribution<double> prob(0.,1.);

    std::cout <<"total size of keys" << n << std::endl;
    ttm::execute_blockwise_parallel(current_block, n,
        [n,  &prob, &reader](size_t s, size_t e)
        {
            std::mt19937_64 re(s*10293903128401092ull);
            reader.getSampleRange(s, e, keys);

        });


    return 0;
}

template <class Hash>
int fill(Hash& hash, size_t n)
{
    auto err = 0u;

    ttm::execute_parallel(current_block, n,
        [&hash, &err](size_t i)
        {
            if (! hash.insert(keys[i], i+2).second) {
                auto data = hash.find(keys[i]);
                if(data == hash.end()){
                    if(err < 5){
                        std::cout  << "how is that possible" << keys[i] << "val " << (*data).second << std::endl;
                    }

                    err++;
                }
              //  ++err;
            }
        });

    if(err > 0){
      //  std::cout << "errors fill " << err << std::endl;
    }
   // errors.fetch_add(err, std::memory_order_relaxed);
    return 0;
}


template <class Hash>
int find_contended(Hash& hash, size_t n)
{
    auto err = 0u;

    ttm::execute_parallel(current_block, n,
        [&hash, &err](size_t i)
        {
            auto key = keys[i];

            auto data = hash.find(keys[i]);

            if (data == hash.end() || keys[(*data).second - 2] != key)
            {
                if(err < 5){
                    std::cout << "errors fill " << "key " << key << " "<< (data == hash.end()) << std::endl;
                }
                ++err;
            }
        });

    if(err > 0){
        std::cout << "errors find contended" << std::endl;
    }
    errors.fetch_add(err, std::memory_order_relaxed);


    return 0;
}

template <class Hash>
int update_contended(Hash& hash, size_t n)
{
    auto err = 0u;

    ttm::execute_parallel(current_block, n,
        [&hash, &err](size_t i)
        {
            auto key = keys[i];

            if (! hash.update(key, growt::example::Overwrite(), i+2).second) ++err;
        });

    if(err > 0){
        std::cout << "errors update" << std::endl;
    }
    errors.fetch_add(err, std::memory_order_relaxed);

    return 0;
}


template <class Hash>
int val_update(Hash& hash, size_t n)
{
    auto err = 0u;

    ttm::execute_parallel(current_block, n,
        [&hash, &err, n](size_t i)
        {
            auto data = hash.find(i+2);
            if      (data == hash.end())
            {   ++ err;   }
            else
            {
                auto temp = (*data).second;
                if (temp != i+2)
                {
                    if (temp < 2   ||
                        temp > n+1 ||
                        keys[temp-2] != i+2) ++err;
                }
            }
        });

    if(err > 0){
        std::cout << "errors in val update" << std::endl;
    }
    // std::cout << " " << err << std::flush;
    errors.fetch_add(err, std::memory_order_relaxed);
    return 0;
}

template <class ThreadType>
struct test_in_stages {

    static int execute(ThreadType t, size_t n, size_t cap, size_t it, dist_reader & reader)
    {

        utils_tm::pin_to_core(t.id);

        using Handle = typename HASHTYPE::Handle;

        if (ThreadType::is_main)
        {
            keys = new uint64_t[n];
        }

        // STAGE0 Create Random Keys
        {
            if (ThreadType::is_main) current_block.store (0);
            t.synchronized(generate_random, n, reader);
        }

        for (size_t i = 0; i<it; ++i)
        {
            // STAGE 0.1
            t.synchronized([cap](bool m)
                           { if (m) hash_table = HASHTYPE(cap); return 0; },
                           ThreadType::is_main);

            t.out << otm::width(3) << i
                  << otm::width(3) << t.p
                  << otm::width(9) << n
                  << otm::width(9) << cap;

            // Needed for synchronization (main thread has finished set_up_hash)
            t.synchronize();

            Handle hash = hash_table.get_handle();
            RobindHoodHandlerWrapper::initIfRobinhoodWrapper(hash, t.p);

            // STAGE2 n Insertions [2 .. n+1]
            {
                if (ThreadType::is_main) current_block.store(0);

                auto duration = t.synchronized(fill<Handle>, hash, n);

                t.out << otm::width(10) << duration.second/1000000.;
            }

            // STAGE4 n Cont Random Updates
            {
                if (ThreadType::is_main) current_block.store(0);

                auto duration = t.synchronized(update_contended<Handle>,
                                               hash, n);


                t.out << otm::width(10) << duration.second/1000000.;
            }


            // STAGE3 n Cont Random Finds Successful
            {
                if (ThreadType::is_main) current_block.store(0);

                auto duration = t.synchronized(find_contended<Handle>, hash, n);

                t.out << otm::width(10) << duration.second/1000000.;
            }


            // STAGE5 Validation of Hash Table Contents
            {
                if (ThreadType::is_main) current_block.store(0);


                 t.out << otm::width(9)  << errors.load();
            }

            t.out << std::endl;
            if (ThreadType::is_main) errors.store(0);
            RobindHoodHandlerWrapper::freeIfRobinhoodWrapper(hash);
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
    size_t n   = c.int_arg("-n", 10000000);
    size_t p   = c.int_arg("-p", 4);
    size_t cap = c.int_arg("-c", n);
    size_t it  = c.int_arg("-it", 5);
    std::string fileName = c.str_arg("-file");

    std::cout << fileName << std::endl;

    dist_reader reader(fileName);
    double con = c.double_arg("-con", 1.0);
    if (! c.report()) return 1;

    otm::out() << otm::width(3)  << "#i"
               << otm::width(3)  << "p"
               << otm::width(9)  << "n"
               << otm::width(9)  << "cap"
               << otm::width(10) << "t_ins_or"
               << otm::width(10) << "t_updt_c"
               << otm::width(10) << "t_find_c"
         //      << otm::width(10) << "t_val_up"
               << otm::width(9)  << "errors"
               << std::endl;

    ttm::start_threads<test_in_stages>(p, n, cap, it, reader);

    return 0;
}
