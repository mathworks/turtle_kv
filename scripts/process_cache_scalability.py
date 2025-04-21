import os
import re
import sys
import matplotlib.pyplot as plt
import numpy as np

from . import turtle_bench


if len(sys.argv) < 2:
    print("Error: must provide at least one argument (the result log to process)",
          file=sys.stderr)
    sys.exit(1);


logs = sys.argv[1:]

combined = {}

for log in logs:
    print(f"Processing {log}...")

    try:
        result = turtle_bench.BenchmarkResult(log)

        print(result.n_records)
        print(result.thruput_k_load)
        print(result.thruput_k_a)
        print(result.thruput_k_b)
        print(result.thruput_k_c)
        print(result.thruput_k_d)
        print(result.thruput_k_e)
        print(result.thruput_k_f)
        print(result.node_hit_rate)
        print([x/2**20 for x in result.mem_used])
        print([x/2**20 for x in result.node_cache])        
        print([sum(x)/2**20 for x in zip(result.node_cache, result.leaf_cache,
                                         result.filter_cache, result.trie_cache)])

        for i in range(result.get_input_count()):
            combined[result.get_input_key(i)] = result.get_output_values(i)

    except:
        raise
        print(f"...incomplete/bad result; skipping")
