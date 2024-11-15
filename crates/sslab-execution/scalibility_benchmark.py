import subprocess
from subprocess import STDOUT
from datetime import datetime
    
class BenchmarkType:
    THROUGHPUT_BLOCK_SIZE = 'measure overall throughput according to blocksize'
    THROUGHPUT_SKEWNESS = 'measure overall throughput according to zipf coef'
    LATENCY_SKEWNESS = 'measure latency according to zipf coef'

    
    

class ExecutionModel:
    OPTME = "-p sslab-execution-optme --features=optme"
    NEZHA = "-p sslab-execution-optme --features=vanilla-kdg"
    BLOCKSTM = "-p sslab-execution-blockstm"
    SERIAL = "-p sslab-execution-serial"
    


benchmarks = [BenchmarkType.THROUGHPUT_BLOCK_SIZE] # [BenchmarkType.LATENCY_SKEWNESS, BenchmarkType.THROUGHPUT_BLOCK_SIZE, BenchmarkType.THROUGHPUT_SKEWNESS] 
execution_models = [ExecutionModel.OPTME, ExecutionModel.BLOCKSTM, ExecutionModel.NEZHA]
num_of_threads = [4, 8, 16, 24, 32]

for model in execution_models:
    for nthreads in num_of_threads:
    
        if BenchmarkType.THROUGHPUT_BLOCK_SIZE in benchmarks:
            
            # measure throughput according to blocksize
            filename = f"{datetime.today().strftime('%Y-%m-%d-%H:%M')}-{nthreads}-blocksize.log"
            cmd = f"RAYON_NUM_THREADS={nthreads} cargo bench {model}" 
            cmd += f" -- blocksize > {filename} 2>&1"
            print(cmd)
            subprocess.call(cmd, shell=True, stderr=STDOUT)
            
            # # parse output 
            # cmd_parsing = f"python3 ./{model}/benches/parse_log.py {filename}"
            # subprocess.call(cmd_parsing, shell=True, stderr=STDOUT)
        
        
        # if BenchmarkType.THROUGHPUT_SKEWNESS in benchmarks \
        #     and workload in (WorkloadType.SMALLBANK, WorkloadType.YOUTUBE):
            
        #     # measure throughput according to skewness    
        #     filename = f"{datetime.today().strftime('%Y-%m-%d-%H:%M')}-{workload}-s{model}-{nthreads}-skewness.log"
        #     cmd = f"WORKLOAD={workload} RAYON_NUM_THREADS={nthreads} cargo bench -p sslab-execution-{model}"
        #     if model == ExecutionModel.OPTME:
        #         cmd += f" --features={model}" 
        #     cmd += f" -- skewness > {filename} 2>&1"
        #     print(cmd)
        #     subprocess.call(cmd, shell=True, stderr=STDOUT)

        #     cmd_parsing = f"python3 ./{model}/benches/parse_log.py {filename}"
        #     subprocess.call(cmd_parsing, shell=True, stderr=STDOUT)
            
        # if BenchmarkType.LATENCY_SKEWNESS in benchmarks \
        #     and model in (ExecutionModel.OPTME, ExecutionModel.BLOCKSTM):
            
        #     # measure latency according to skewness    
        #     filename = f"{datetime.today().strftime('%Y-%m-%d-%H:%M')}-{workload}-s{model}-{nthreads}-latency.log"
        #     cmd = f"WORKLOAD={workload} RAYON_NUM_THREADS={nthreads} cargo bench -p sslab-execution-{model}"
        #     cmd += f" --features=latency"
        #     cmd += f" -- {model} > {filename} 2>&1"
        #     print(cmd)
        #     subprocess.call(cmd, shell=True, stderr=STDOUT)
            
        #     cmd_parsing = f"python3 ./{model}/benches/parse_log.py {filename}"
        #     subprocess.call(cmd_parsing, shell=True, stderr=STDOUT)
            