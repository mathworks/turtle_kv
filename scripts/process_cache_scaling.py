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

        fig, ax = plt.subplots()
        series = []

        cache_size_gb = [z / 2**30 for z in result.cache_size]
        
        ax.set_title("Cache-Limited Performance\n"
                     f"(N={50}M, k:v={24}:{100})")

        series += ax.plot(cache_size_gb, result.thruput_k_load,
                  label='Load KOps/Second')

        series += ax.plot(cache_size_gb, result.thruput_k_a,
                  label='A (50/50, Zipfian)')

        series += ax.plot(cache_size_gb, result.thruput_k_b,
                  label='B (95/5, Zipfian)')

        series += ax.plot(cache_size_gb, result.thruput_k_c,
                  label='C (100/0, Zipfian)')

        series += ax.plot(cache_size_gb, result.thruput_k_d,
                  label='D (95/5, Latest)')

        series += ax.plot(cache_size_gb, result.thruput_k_f,
                  label='F (95/5 (r/m/w), Zipfian)')

        ax.grid()
        ax.grid(which="minor", color="0.9")
        
        ax.set_ylim(0, 1600)
        ax.set_xlabel("Cache Size (GB)")
        ax.set_ylabel("KOps/Second")
        
        ax.legend(series, [s.get_label() for s in series], loc='lower right')
        
        plt.savefig(log + ".png")
        
    except:
        raise
        print(f"...incomplete/bad result; skipping")


