import os
import re
import sys
from statistics import mean, stdev
from itertools import zip_longest

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
per_chi = {}

for log in logs:
    print(f"Processing {log}...")

    try:
        result = turtle_bench.BenchmarkResult(log)
        results.append(result)

        print("cache", result.cache_size)
        print("χ", result.chi)
        print("Ν", result.n_records)
        print("load", result.thruput_k_load)
        print("a", result.thruput_k_a)
        print("b", result.thruput_k_b)
        print("c", result.thruput_k_c)
        print("d", result.thruput_k_d)
        print("f", result.thruput_k_f)
        
        combined = list(zip_longest(
            result.chi,
            result.thruput_k_load,
            result.thruput_k_a,
            result.thruput_k_b,
            result.thruput_k_c,
            result.thruput_k_d,
            result.thruput_k_f,
            fillvalue=0,
        ))

        for i, chi in enumerate(result.chi):
            if chi not in per_chi:
                per_chi[chi] = []

            per_chi[chi].append(combined[i])
        
        for c in combined:
            print(c)
        
    except:
        raise
        print(f"...incomplete/bad result; skipping")

#for chi, vals in per_chi.items():
#    zl = [int(round(mean([x for x in xs if x != 0]))) for xs in  zip_longest(*vals)]
#    print(zl)
