import argparse
import subprocess
import os
import matplotlib.pyplot as plt
import numpy as np

ID_TO_TABLE = {'f':'folly', 'fo': 'folklore', 'c':'cuckoo', 'r':'robinhood', 's':'ska', 'sg':'usGrowT', 'ag': 'uaGrowT', 'th': 'TBBhm', 'tu':'TBBum', 'jg': 'junction_grampa', 'jl': 'junction_leap', 'ji':'junction_linear'}
ID_TO_TABLE_NAME = {'f':'folly', 'fo': 'folklore', 'c':'cuckoo', 'r':'Bolt', 's':'ska', 'sg':'usGrowt', 'ag': 'uaGrowt', 'th': 'TBBhm', 'tu':'TBBum', 'jg': 'junction_grampa', 'jl': 'junction_leap', 'ji':'junction_linear'}
#TREAT_SAME = {{'ag', 'sg'}}
ID_TO_COLOR = {'f':'b', 'c':'m', 'fo': 'blue', 'r':'g', 's':'y', 'sg':'c', 'ag':'r', 'th': 'purple', 'tu':'hotpink', 'jg': 'orange', 'jl': 'gold', 'ji':'black'}
ID_TO_MARKER = {'f':'o', 'c':'v', 'fo': '1', 'r':'P', 's':'D', 'sg':'*','ag':'X', 'th':'+', 'tu': 'H', 'jg': 'x', 'jl': '|', 'ji':'_'}
ID_TO_BENCHMARK = {'i': "ins_benchmark", 'd': "del_benchmark", 'm': "mix_benchmark", 'c': "con_benchmark"}
THREAD_NUMS = []
DATA = {}
BENCHMARK_TO_COLS = {"i":["t_ins","t_find_-","t_find_+"], "d":["t_del"], "m":["t_mix"], "c":["t_mix","t_updt_c"]}
ID_TO_TITLE = {'i': {"t_ins":"Runtime of Concurrent Inserts",
                     "t_find_-": "Runtime of Finding Nonexistant Keys",
                     "t_find_+": "Runtime of Finding Existant Keys"},
               'd': {"t_del": "Runtime of Concurrent Deletes"},
               'm': {"t_mix": "Runtime of Mixed Concurrent Operations"},
               'c': {"t_mix": "Runtime of Mixed Concurrent Operations",
                     "t_updt_c": "Runtime of Update Concurrent Operations"}}

ins_benchmark_exec_cmd = "./ins/ins_full_{} -n {}  -p {} -it {};"

ins_benchmark_c_exec_cmd = "./ins/ins_full_{} -n {} -c {} -p {} -it {};"
con_benchmark_exec_cmd = "./con/con_full_{} -file {} -wperc {} -n {} -p {} -it {}"
del_benchmark_exec_cmd = "./del/del_full_{} -n {} -p {} -it {};"
mix_benchmark_exec_cmd = "./mix/mix_full_{} -n {} -c {} -stream {} -p {} -it {} -wperc {};"

###################
# Parsing arguments
###################

default_cap = 1

parser = argparse.ArgumentParser(description='Parsing parameters to run and graph benchmarks')
parser.add_argument('-b', '--benchmarks', type=str, default='',
                    help='which benchmarks need to be run')
parser.add_argument('-t', '--tables', type=str, default='frgc', nargs='+',
                    help='tables that need to be graphed and benchmarked')
parser.add_argument('-rp', '--range-num-threads', type=int, nargs=2, default=None,
                    help='number of threads as a range, doubling')
parser.add_argument('-lp', '--list-num-threads', type=int, nargs='+', default=None,
                    help='number of threads, as a list')
parser.add_argument('-n', '--num-elem', type=int, default=1,
                    help='number of elements to be inserted')
parser.add_argument('-c', '--capacity', type=int, default=default_cap,
                    help='initial size of hashtable')
parser.add_argument('-it', '--iterations', type=int, default=1,
                    help='number of iterations to run tests')
parser.add_argument('-stream', '--stream-size', type=int, default=40000000,
                    help='stream size used for mix test')
parser.add_argument('-wperc', '--wperc', type=float, default=0.1,
                    help='stream size used for mix test')
parser.add_argument('-to', '--table-outfile', type=str, default=None,
                    help='outfile used to store latex code of table')
parser.add_argument('-rd', '--result-directory', type=str, default='resultsr',
                    help='outfile used to store latex code of table')
parser.add_argument('-tmc', '--table-max-core', type=int, default=4,
                    help='# of cores to compare serial time with')
parser.add_argument('-wh', '--write-header', type=bool, default=False,
                    help='write latex table header as well')
parser.add_argument('-rf', '--read-from-file', type=bool, default=False,
                    help='write latex table header as well')
parser.add_argument('-tpc', '--max-tables-per-col', type=int, default=3,
                    help='make algos to put per latex table')
parser.add_argument('-dist', '--dist', type=str, default="\"/home/gokusper/Downloads/pbbs/bwt/sequenceData/zipf.txt\"",
                    help='file of distribution to use for keys')
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


print("Benchmarks:", [ID_TO_BENCHMARK[b] for b in args.benchmarks])
print("Tables:", [ID_TO_TABLE[t] for t in args.tables])
print("Number of Threads:", THREAD_NUMS)
print("Number of Elements:", args.num_elem)
print("Initial capacity:", args.capacity)

dirName = args.result_directory

try:
    # Create target Directory
    os.mkdir(dirName)
    print("Directory " , dirName ,  " Created ")
except OSError as e:
    pass

# add ska, just to find relative speedup
if 1 in THREAD_NUMS:
    args.tables += "s"

args.iterations += 1


for benchmark in args.benchmarks:
        DATA[benchmark] = {}
        for col in BENCHMARK_TO_COLS[benchmark]:
                DATA[benchmark][col] = {}
                for ID in args.tables:
                        DATA[benchmark][col][ID_TO_TABLE[ID]] = {}
                        for num_threads in THREAD_NUMS:
                                DATA[benchmark][col][ID_TO_TABLE[ID]][num_threads] = []

if not args.read_from_file:
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
                                            if args.capacity == default_cap :
                                                f.write(ins_benchmark_exec_cmd.format(table, args.num_elem, num_threads, args.iterations))
                                            else:
                                                f.write(ins_benchmark_c_exec_cmd.format(table, args.num_elem, args.capacity, num_threads, args.iterations))
                                    elif benchmark == "d":
                                            f.write(del_benchmark_exec_cmd.format(table, args.num_elem,
                                                                                                                      num_threads, args.iterations))
                                    elif benchmark == "m":
                                            f.write(mix_benchmark_exec_cmd.format(table, args.num_elem, args.capacity,
                                                                                                                      args.stream_size, num_threads,
                                                                                                                  args.iterations, args.wperc))
                                    elif benchmark == "c":
                                            f.write(con_benchmark_exec_cmd.format(table, args.dist, args.wperc, args.num_elem, num_threads, args.iterations))
                                    f.write("\n")
            subprocess.run(["chmod", "+x", ID_TO_BENCHMARK[benchmark]+".sh"])

####################
# Running benchmarks
####################
    print("Running scripts...")
    for benchmark in args.benchmarks:
            os.system("./"+ID_TO_BENCHMARK[benchmark]+".sh > "+ID_TO_BENCHMARK[benchmark]+".out")
else:
    print("Read from File enabled. Reading from...")
    for benchmark in args.benchmarks:
        print("\t"+ID_TO_BENCHMARK[benchmark]+".out")

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
                                        op_per_event = 1
                                        if benchmark == "d":
                                            op_per_event = 2
                                        if benchmark == "m":
                                            DATA[benchmark][col][current_table][int(data["p"])].append(args.stream_size/(1000*float(data[col])))
                                        else:
                                            DATA[benchmark][col][current_table][int(data["p"])].append(args.num_elem*op_per_event/(1000*float(data[col])))
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
                            val = np.median(np.array(all_trials[1:]))
                            Y_VARS[benchmark][col][table].append(val)

def get_name(ID):
    return ID_TO_TABLE_NAME[ID]

for benchmark in args.benchmarks:
    for col in BENCHMARK_TO_COLS[benchmark]:
        for ID in args.tables:
            if ID == "s":
                continue
            plt.plot(x_vars, Y_VARS[benchmark][col][ID_TO_TABLE[ID]],marker = ID_TO_MARKER[ID], c = ID_TO_COLOR[ID], label = get_name(ID))
            #plt.scatter(x_vars, Y_VARS[benchmark][col][ID_TO_TABLE[ID]], c = ID_TO_COLOR[ID])
        plt.tight_layout(rect=[0,0,1.0,1])
        #copy_thread_nums = THREAD_NUMS
        #copy_thread_nums.remove(2)
        #plt.xticks(copy_thread_nums)
        plt.xticks(THREAD_NUMS)
        plt.legend(bbox_to_anchor=(1.01, 1), loc='upper left')
      #  plt.title(ID_TO_TITLE[benchmark][col])
        plt.xlabel("Number of threads")
        plt.ylabel("Throughput (MOps/sec)")
        plt.savefig(dirName+'//'+ID_TO_BENCHMARK[benchmark]+"_"+col+'.png', bbox_inches="tight")
        plt.clf()

########################
# Comparing to Robinhood
########################
print("Comparing to Robinhood...")
os.system("rm " + dirName+'//'+ "robinhood_"+ID_TO_BENCHMARK[benchmark]+".out")

with open(dirName+'//'+"robinhood_"+ID_TO_BENCHMARK[benchmark]+".out", "a") as f:
    header = ["table_algo"]
    for benchmark in args.benchmarks:
        for col in BENCHMARK_TO_COLS[benchmark]:
            header.append(col)
    f.write('\t'.join(header))
    f.write('\n')

    for ID in args.tables:
        if ID == "s" or ID == "r":
            continue
        table = ID_TO_TABLE[ID]
        line = [table]
        print(ID)
        for benchmark in args.benchmarks:
            for col in BENCHMARK_TO_COLS[benchmark]:

                line.append(str(round(Y_VARS[benchmark][col]["robinhood"][-1]/Y_VARS[benchmark][col][table][-1], 2)))

        f.write("\t".join(line))
        f.write('\n')

    #f.write("Speedup " + str(round(Y_VARS[benchmark][col]["robinhood"][-1]/Y_VARS[benchmark][col]["robinhood"][0], 2)))

    f.write("Absolute speedup" + str(round(Y_VARS[benchmark][col]["robinhood"][-1]/Y_VARS[benchmark][col]["ska"][0], 2)))

###################
# Making Table
###################
def format_col(col):
    c = col.split("_")
    return '\\_'.join(c)

print("Making table line...")
if args.table_outfile:
    single_core_index = THREAD_NUMS.index(1)
    mult_core_index = THREAD_NUMS.index(args.table_max_core)
    half_mult_core_index = THREAD_NUMS.index(args.table_max_core/2)
    if -1 in [single_core_index, mult_core_index, half_mult_core_index]:
        print("ERROR: Core specified is missing!")
    for table_id in range((len(args.tables)+args.max_tables_per_col+1)//args.max_tables_per_col):
        with open(args.table_outfile+"_"+str(table_id), 'a') as f:
            tables = args.tables[table_id*args.max_tables_per_col:min((table_id+1)*args.max_tables_per_col, len(args.tables))]
            if args.write_header:
                f.write("\\begin{tabular}{|l|"+("c|"*(len(tables)*4+1))+"}\n")
                f.write("\\hline\n")
                f.write("\\multicolumn{"+str(len(tables)*4+1)+"}{|c|}{Name of Table} \\\\\n")
                f.write("\\hline\n")
                f.write("\\multicolumn{1}{|c|}{Configurations} ")
                for ID in tables:
                    if ID == "s":
                        continue
                    f.write("& \\multicolumn{4}{c|}{"+get_name(ID)+"} ")
                f.write("\\\\\n")
                f.write("\\cline{2-"+str(len(tables)*4+1)+"}\n")
                f.write("\\multicolumn{1}{|c|}{  } ")
                for ID in tables:
                    if ID == "s":
                        continue
                    f.write("& T_{"+str(args.table_max_core//2)+"} & SU & SKA SU & RH SU ")
                f.write("\\\\\n")
                f.write("\\hline\n")

            for benchmark in args.benchmarks:
                for col in BENCHMARK_TO_COLS[benchmark]:
                    if benchmark != "m":
                        line = "{}(n={}M) ".format(format_col(col), round(float(args.num_elem)/1000000,1))
                    else:
                        line = "{}(n={}M, wperc={}) ".format(format_col(col), round(float(args.num_elem)/1000000,1), args.wperc)
                    ska_val = Y_VARS[benchmark][col]["ska"][0]
                    for ID in tables:
                        if ID == "s":
                            continue
                        row_data = Y_VARS[benchmark][col][ID_TO_TABLE[ID]]
                        mult_val_to_use = max(row_data[mult_core_index], row_data[half_mult_core_index]) # get best thruput
                        rh_su = Y_VARS[benchmark][col]["robinhood"][mult_core_index]/mult_val_to_use
                        line += "& {} & {} & {} & {} ".format(round(mult_val_to_use,2),
                                                              round(mult_val_to_use/row_data[single_core_index],2),
                                                              round(mult_val_to_use/ska_val,2),
                                                              round(rh_su,2))
                    """
                    line += "& {} ".format(Y_VARS[benchmark][col]["ska"][0]) # always serial
                    for ID in args.tables:
                        if ID == "s":
                            continue
                        row_data = Y_VARS[benchmark][col][ID_TABLE[ID]]
                        line += "& {} & {} & {} ".format(row_data[single_core_index],
                                                         row_data[mult_core_index],
                                                         row_data[single_core_index]/row_data[mult_core_index])
                    """
                    line += "\\\\\n"
                    f.write(line)

            f.write("\\hline\n")




