
echo $PERF_DEBUG

cmake -DASSERT=Off -DDEBUG_HASH=Off -DPERF_DEBUG=Off -DREMOTE=Off -DDEBUG=Off .
make -j



