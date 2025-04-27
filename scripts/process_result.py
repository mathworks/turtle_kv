import os
import re
import sys
from statistics import mean, stdev

import matplotlib.pyplot as plt
import numpy as np

import turtle_bench


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

def average_result(attribute_name):
    return list(map(mean, zip(*(getattr(result, attribute_name) for result in results))))

def stdev_result(attribute_name):
    return list(map(stdev, zip(*(getattr(result, attribute_name) for result in results))))

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

if len(sys.argv) < 2:
    print("Error: must provide at least one argument (the result log to process)",
          file=sys.stderr)
    sys.exit(1);


logs = sys.argv[1:]

combined = {}

results = []

for log in logs:
    print(f"Processing {log}...")

    try:
        result = turtle_bench.BenchmarkResult(log)
        results.append(result)

        print(result.cache_size)
        print(result.chi)
        print(result.n_records)
        print(result.thruput_k_load)
        print(result.thruput_k_a)
        print(result.thruput_k_b)
        print(result.thruput_k_c)
        print(result.thruput_k_d)
        print(result.thruput_k_f)

        combined = [(chi, tp_load) for chi, tp_load in zip(result.chi, result.thruput_k_load)]

        for c in combined:
            print(c)
        
    except:
        print(f"...incomplete/bad result; skipping")
