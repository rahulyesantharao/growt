#!/bin/sh

nice -1 ./ins/ins_full_robinhood -n 10000000 -p 1 -it 2; 
#nice -1 ./ins/ins_full_robinhood -n 10000000 -p 2 -it 2;
nice -1 ./ins/ins_full_robinhood -n 10000000 -p 4 -it 2;
#<<COMMENT
./del/del_full_robinhood -n 10000000 -p 1 -it 2;
#./del/del_full_robinhood -n 10000000 -p 2 -it 2;
./del/del_full_robinhood -n 10000000 -p 4 -it 2;
./mix/mix_full_robinhood -n 50000 -c 10000000 -stream 40000000  -p 1 -it 2 -wperc 0.1; 
./mix/mix_full_robinhood -n 50000 -c 10000000 -stream 40000000  -p 4 -it 2 -wperc 0.1;

./mix/mix_full_robinhood -n 50000 -c 10000000 -stream 40000000  -p 1 -it 2 -wperc 0.1;
./mix/mix_full_robinhood -n 50000 -c 10000000 -stream 40000000  -p 4 -it 2 -wperc 0.5;

#resize benchmark
./ins/ins_full_robinhood -c 1000000 -n 10000000 -p 1 -it 2


#COMMENT

