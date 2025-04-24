import os
import re
import sys
from statistics import mean, stdev

import matplotlib.pyplot as plt
import numpy as np

import turtle_bench


if len(sys.argv) < 2:
    print("Error: must provide at least one argument (the result log to process)",
          file=sys.stderr)
    sys.exit(1);


logs = sys.argv[1:]

results = []

for log in logs:
    print(f"Processing {log}...")

    try:
        result = turtle_bench.BenchmarkResult(log)
        results.append(result)

        n = result.n_records[0]
        gets_per_read = [(n) / (r*n+1) for r in result.reads_per_get_4k]
        
        print(result.cache_size)
        print(result.mem_used)
        print(result.n_records)
        print(result.thruput_k_load)
        print(result.thruput_k_a)
        print(result.thruput_k_b)
        print(result.thruput_k_c)
        print(result.thruput_k_d)
        print(result.thruput_k_f)
        print(result.reads_per_get_4k)
        print(result.cache_hit_rate)
        print(gets_per_read)
    except:
        raise
        print(f"...incomplete/bad result; skipping")


