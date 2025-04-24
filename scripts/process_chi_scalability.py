import os
import re
import sys
import matplotlib.pyplot as plt
import numpy as np

import turtle_bench


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
        result.plot_page_faults()
        result.plot_mem_used()
        result.plot_cpu_util()
        result.plot_cache_miss_rate()

        for i in range(result.get_input_count()):
            combined[result.get_input_key(i)] = result.get_output_values(i)

    except:
        print(f"...incomplete/bad result; skipping")
