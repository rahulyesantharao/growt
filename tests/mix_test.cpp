/*******************************************************************************
 * tests/mix_test.cpp
 *
 * mixed inserts and finds test for more information see below
 *
 * Part of Project growt - https://github.com/TooBiased/growt.git
 *
 * Copyright (C) 2015-2016 Tobias Maier <t.maier@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include "tests/selection.h"

#include "wrapper/robinhood_wrapper.h"
#include "utils/default_hash.hpp"
#include "utils/thread_coordination.hpp"
#include "utils/pin_thread.hpp"
#include "utils/command_line_parser.hpp"
#include "utils/output.hpp"

#include "utils/random_mix.hpp"

#include <random>
#include <unordered_map>
/*
 * This Test is meant to test the tables performance on uniform random inputs.
 * 0. Creating 2n random keys
 * 1. Inserting n elements (key, index) - the index can be used for validation
 * 2. Looking for n elements - using different keys (likely not finding any)
 * 3. Looking for the n inserted elements (hopefully finding all)
 *    (correctness test using the index)
 */

const static uint64_t range     = (1ull << 62) -1;
namespace otm = utils_tm::out_tm;
namespace ttm = utils_tm::thread_tm;

alignas(64) static HASHTYPE hash_table = HASHTYPE(0);
alignas(64) static uint64_t* keys;
static Test::random_mix_generator * gen;
alignas(64) static Test::random_mix_generator::Event * operations;
alignas(64) static Test::random_mix_generator::Event * operations_copy;
alignas(64) static std::atomic_size_t current_block;
alignas(64) static std::atomic_size_t out_of_order_count;
alignas(64) static std::atomic_size_t unsucc_finds;

//fraction of entries that could be processed out of order with (p >1) causing errors
const static double out_of_order_ratio = 0.0001;

typedef Test::Op Op;

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

int copy(size_t n){
    ttm::execute_blockwise_parallel(current_block,n,
                                    [n](size_t s, size_t e)
                                    {
                                        for(size_t i = s; i < e; i++){
                                            operations_copy[i] = operations[i];
                                        }
                                    });
     return 0;
}

bool verify_dist(size_t stream_size, size_t  inital_size){
    std::unordered_map<size_t,size_t> count;
    for(size_t i = 0; i < stream_size; i++){
        count[operations[i].key] += 1;
    }

    double rand_threshold = (stream_size)/(double)(inital_size);
    bool verify = true;
    std::cout << "threshold " << rand_threshold << std::endl;
    for(auto entries: count){
        if((entries.second) >= 10*rand_threshold){
            std::cout << "hjow " << entries.second << std::endl;
            verify = false;
        }
    }

    return verify;
}

void generateEvents(size_t stream_size, size_t initial_size ){
    for(size_t i = 0; i < stream_size; i++){
        operations[i] = gen->getNextEntry();
    }

    #ifdef DEBUG_HASH
        verify_dist(stream_size, initial_size);
    #endif
}

template <class Hash>
int executeEvents(Hash & hash, size_t end){
    auto err = 0u;


    size_t counter = 0;
    size_t find = 0;
    size_t delete_ = 0;
    size_t insert_ = 0;

    ttm::execute_parallel(current_block, end,
                              [&hash, &err, &counter, &find, &delete_, &insert_](size_t i)
                              {
                                  auto e = operations_copy[i];
                                  switch(e.op){
                                      case Op::Find: {
                                          auto data = hash.find(e.key);
                                          err += data == hash.end();
                                          find += data == hash.end();
                                          break;
                                      }
                                      case Op::Delete: {
                                          bool success = hash.erase(e.key);
                                          err += !success;
                                          delete_ += !success;
                                          break;
                                      }
                                      case Op::Insert: {
                                          auto temp = hash.insert(e.key, i);
                                          err += !temp.second;
                                          insert_ += !temp.second;
                                          break;
                                      }
                                  }
         });


    out_of_order_count.fetch_add(err, std::memory_order_relaxed);

    return 0;
}


template <class Hash>
int prefill(Hash& hash, size_t pre)
{
    auto err = 0u;

    ttm::execute_parallel(current_block, pre,
                          [&hash, &err](size_t i)
                          {
                              auto key = keys[i];
                              auto temp = hash.insert(key, i+2);
                              if (! temp.second)
                              { ++err; }
                          });

    out_of_order_count.fetch_add(err, std::memory_order_relaxed);
    return 0;
}


template <class ThreadType>
struct test_in_stages
{

    static int execute(ThreadType t, size_t n, size_t stream_size, size_t cap, double wperc,
            size_t it)
    {
        using Handle = typename HASHTYPE::Handle;

        utils_tm::pin_to_core(t.id);

        if (ThreadType::is_main)
        {
            keys = new uint64_t[n];
            operations = new Test::random_mix_generator::Event[stream_size];
            operations_copy = new Test::random_mix_generator::Event[stream_size];
            gen = new Test::random_mix_generator (keys, n, wperc);
        }
        {
            if (ThreadType::is_main) current_block.store (0);
            t.synchronized(generate_random, n);
        }
        {
            for (size_t i = 0; i < it; ++i)
            {

                // STAGE 0.011
                t.synchronized([cap](bool m)
                               { if (m) hash_table = HASHTYPE(cap); return 0; },
                               ThreadType::is_main);
                t.synchronized([n, wperc](bool m)
                               { if (m)  gen = new Test::random_mix_generator (keys, n, wperc); return 0; },
                               ThreadType::is_main);
                t.synchronized([cap, stream_size, n](bool m)
                               { if (m) generateEvents(stream_size, n); return 0; },
                               ThreadType::is_main);
                if (ThreadType::is_main) current_block.store (0);
                t.synchronized(copy,stream_size);
                t.out << otm::width(3) << i
                      << otm::width(3) << t.p
                      << otm::width(9) << n
                      << otm::width(9) << cap
                      << otm::width(6) << wperc;

                t.synchronize();

                Handle hash = hash_table.get_handle();


                t.synchronize();
                RobindHoodHandlerWrapper::initIfRobinhoodWrapper(hash, t.p);
                t.synchronize();
                {
                    if (ThreadType::is_main) current_block.store(0);

                    t.synchronized(prefill<Handle>, hash, n);
                }


                {
                    if (ThreadType::is_main) current_block.store(0);

                    auto duration = t.synchronized(executeEvents<Handle>, hash, stream_size);


                    bool success = out_of_order_count <= out_of_order_ratio * stream_size * (t.p - 1);

                    t.out << otm::width(10) << duration.second/1000000.
                          << otm::width(10) << out_of_order_count.load()
                          << otm::width(7) << !success;
                }

                if (ThreadType::is_main)
                {
                    out_of_order_count.store(0);
                    unsucc_finds.store(0);
                }

                t.out << std::endl;
                RobindHoodHandlerWrapper::freeIfRobinhoodWrapper(hash);
            }

            if (ThreadType::is_main)
            {
                delete[] keys;
                delete [] operations;
                delete [] operations_copy;
                delete gen;
            }


            return 0;
        }


    }
};


int main(int argn, char** argc)
{
    utils_tm::command_line_parser c{argn, argc};
    size_t n     = c.int_arg("-n"  , 10000000);
    size_t p     = c.int_arg("-p"  , 4);
    size_t it    = c.int_arg("-it" , 5);
    double wperc = c.double_arg("-wperc", 0.1);
    size_t stream_size = c.int_arg("-stream", n);
    size_t cap   = c.int_arg("-c"  , n);
    if (! c.report()) return 1;

    otm::out() << otm::width(3)  << "#i"
               << otm::width(3)  << "p"
               << otm::width(9)  << "n"
               << otm::width(9)  << "cap"
               << otm::width(6)  << "w_per"
               << otm::width(10) << "t_mix"
               << otm::width(10) << " out_of_order"
               << otm::width(7)  << "error"
               << std::endl;


    ttm::start_threads<test_in_stages>(p, n, stream_size, cap, wperc, it);

    return 0;
}
