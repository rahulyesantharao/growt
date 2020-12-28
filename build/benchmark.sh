#!/bin/sh

echo "insertion benchmark.." 
nice -1 ./ins/ins_full_robinhood -n 10000000 -p 1 -it 2; 
#nice -1 ./ins/ins_full_robinhood -n 10000000 -p 2 -it 2;
nice -1 ./ins/ins_full_robinhood -n 10000000 -p 4 -it 2;

echo "deletion benchmark..."
./del/del_full_robinhood -n 10000000 -p 1 -it 2;
./del/del_full_robinhood -n 10000000 -p 4 -it 2;

echo "mix benchmark 10% write ..."
./mix/mix_full_robinhood -n 50000 -c 10000000 -stream 40000000  -p 1 -it 2 -wperc 0.1; 
./mix/mix_full_robinhood -n 50000 -c 10000000 -stream 40000000  -p 4 -it 2 -wperc 0.1;

echo "mix benchmark 50% write ..."
./mix/mix_full_robinhood -n 50000 -c 10000000 -stream 40000000  -p 1 -it 2 -wperc 0.5;
./mix/mix_full_robinhood -n 50000 -c 10000000 -stream 40000000  -p 4 -it 2 -wperc 0.5;

#resize benchmark
./ins/ins_full_robinhood -c 1000000 -n 10000000 -p 1 -it 2;

#COMMENT

