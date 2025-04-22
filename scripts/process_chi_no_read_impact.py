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
        print(result.thruput_k_c)
        print(result.chi)
        print(result.mem_used)
        print(result.cpu_util)

    except:
        print(f"...incomplete/bad result; skipping")
