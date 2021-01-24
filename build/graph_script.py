import argparse
import subprocess
import os
import matplotlib.pyplot as plt
import numpy as np

ID_TO_TABLE = {'f':'folly', 'c':'cuckoo', 'r':'robinhood', 's':'ska', 'g':'paGrowT'}
ID_TO_COLOR = {'f':'b', 'c':'m', 'r':'g', 's':'y', 'g':'c'}
ID_TO_BENCHMARK = {'i': "ins_benchmark", 'd': "del_benchmark", 'm': "mix_benchmark"}
THREAD_NUMS = []
DATA = {}
BENCHMARK_TO_COLS = {"i":["t_ins","t_find_-","t_find_+"], "d":["t_del"], "m":["t_mix"]}
ID_TO_TITLE = {'i': {"t_ins":"Runtime of Concurrent Inserts",
                     "t_find_-": "Runtime of Finding Nonexistant Keys",
                     "t_find_+": "Runtime of Finding Existant Keys"},
               'd': {"t_del": "Runtime of Concurrent Deletes"},
               'm': {"t_mix": "Runtime of Mixed Concurrent Operations"}}

ins_benchmark_exec_cmd = "./ins/ins_full_{} -n {} -p {} -it {};"
del_benchmark_exec_cmd = "./del/del_full_{} -n {} -p {} -it {};"
mix_benchmark_exec_cmd = "./mix/mix_full_{} -n {} -c {} -stream {} -p {} -it {} -wperc {};"

###################
# Parsing arguments
###################
parser = argparse.ArgumentParser(description='Parsing parameters to run and graph benchmarks')
parser.add_argument('-b', '--benchmarks', type=str, default='',
                    help='which benchmarks need to be run')
parser.add_argument('-t', '--tables', type=str, default='frgc',
                    help='tables that need to be graphed and benchmarked')
parser.add_argument('-rp', '--range-num-threads', type=int, nargs=2, default=None,
                    help='number of threads as a range, doubling')
parser.add_argument('-lp', '--list-num-threads', type=int, nargs='+', default=None,
                    help='number of threads, as a list')
parser.add_argument('-n', '--num-elem', type=int, default=1,
                    help='number of elements to be inserted')
parser.add_argument('-c', '--capacity', type=int, default=8,
                    help='initial size of hashtable')
parser.add_argument('-it', '--iterations', type=int, default=1,
                    help='number of iterations to run tests')
parser.add_argument('-stream', '--stream-size', type=int, default=40000000,
                    help='stream size used for mix test')
parser.add_argument('-wperc', '--wperc', type=float, default=0.1,
                    help='stream size used for mix test')
args = parser.parse_args()

#####################
# Formatting metadata
#####################
if args.range_num_threads:
        num = args.range_num_threads[0]
        while num <= args.range_num_threads[1]:
                THREAD_NUMS.append(num)
                num *= 2
elif args.list_num_threads:
        THREAD_NUMS = args.list_num_threads
else:
        print("ERROR:NO THREAD NUMS SPECIFIED")

for benchmark in args.benchmarks:
        DATA[benchmark] = {}
        for col in BENCHMARK_TO_COLS[benchmark]:
                DATA[benchmark][col] = {}
                for ID in args.tables:
                        DATA[benchmark][col][ID_TO_TABLE[ID]] = {}
                        for num_threads in THREAD_NUMS:
                                DATA[benchmark][col][ID_TO_TABLE[ID]][num_threads] = []

print("Benchmarks:", [ID_TO_BENCHMARK[b] for b in args.benchmarks])
print("Tables:", [ID_TO_TABLE[t] for t in args.tables])
print("Number of Threads:", THREAD_NUMS)
print("Number of Elements:", args.num_elem)
print("Initial capacity:", args.capacity)

# add ska, just to find relative speedup
args.tables += "s"

####################
# Setting up scripts
####################
print("Making scripts...")
for benchmark in args.benchmarks:
        # remove previous versions of the script
        subprocess.run(["rm", ID_TO_BENCHMARK[benchmark]+".sh"])
        with open(ID_TO_BENCHMARK[benchmark]+".sh", "a") as f:
                for ID in args.tables:
                        table = ID_TO_TABLE[ID]
                        f.write("echo TABLE: {};\n".format(table))
                        for num_threads in THREAD_NUMS:
                                if ID == "s" and num_threads > 1:
                                    continue
                                if benchmark == "i":
                                        f.write(ins_benchmark_exec_cmd.format(table, args.num_elem,
                                                                                                                  num_threads, args.iterations))
                                elif benchmark == "d":
                                        f.write(del_benchmark_exec_cmd.format(table, args.num_elem,
                                                                                                                  num_threads, args.iterations))
                                elif benchmark == "m":
                                        f.write(mix_benchmark_exec_cmd.format(table, args.num_elem, args.capacity,
                                                                                                                  args.stream_size, num_threads,
                                                                                                              args.iterations, args.wperc))
                                f.write("\n")
        subprocess.run(["chmod", "+x", ID_TO_BENCHMARK[benchmark]+".sh"])

####################
# Running benchmarks
####################
print("Running scripts...")
for benchmark in args.benchmarks:
        os.system("./"+ID_TO_BENCHMARK[benchmark]+".sh > "+ID_TO_BENCHMARK[benchmark]+".out")

###################
# Parsing outputs
###################
def get_data(raw_line, header):
        data = {}
        line = raw_line.split()
        for i in range(len(line)):
                data[header[i]] = line[i]
        return data

print("Parsing output...")
for benchmark in args.benchmarks:
        with open(ID_TO_BENCHMARK[benchmark]+".out") as f:
                current_table = ""
                current_header = []
                for line in f:
                        if line.startswith("TABLE"):
                                current_table = line.split()[1]
                        elif line.startswith(" #i"):
                                current_header = line.split()
                        elif len(current_header) == len(line.split()):
                                data = get_data(line, current_header)
                                for col in BENCHMARK_TO_COLS[benchmark]:
                                        DATA[benchmark][col][current_table][int(data["p"])].append(float(data[col]))

###################
# Graphing Results
###################
print("Graphing output...")
x_vars = THREAD_NUMS
Y_VARS = {}
for benchmark in args.benchmarks:
        Y_VARS[benchmark] = {}
        for col in BENCHMARK_TO_COLS[benchmark]:
                Y_VARS[benchmark][col] = {}
                for ID in args.tables:
                        table = ID_TO_TABLE[ID]
                        Y_VARS[benchmark][col][table] = []
                        for p in THREAD_NUMS:
                            all_trials = {}
                            if ID == "s":
                                all_trials = DATA[benchmark][col][table][1]
                            else:
                                all_trials = DATA[benchmark][col][table][p]
                            val = np.array(all_trials).mean()
                            Y_VARS[benchmark][col][table].append(val)

for benchmark in args.benchmarks:
    for col in BENCHMARK_TO_COLS[benchmark]:
        for ID in args.tables:
            if ID == "s":
                continue
            plt.plot(x_vars, Y_VARS["i"]["t_ins"][ID_TO_TABLE[ID]], c = ID_TO_COLOR[ID], label = ID_TO_TABLE[ID])
            plt.scatter(x_vars, Y_VARS["i"]["t_ins"][ID_TO_TABLE[ID]], c = ID_TO_COLOR[ID])
        plt.xticks(THREAD_NUMS)
        plt.legend(loc='upper right')
        plt.title(ID_TO_TITLE[benchmark][col])
        plt.xlabel("Number of threads")
        plt.ylabel("Runtime (miliseconds)")
        plt.savefig(ID_TO_BENCHMARK[benchmark]+"_"+col+'.png')
        plt.clf();









