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

combined = {}

results = []

for log in logs:
    print(f"Processing {log}...")

    try:
        results.append(turtle_bench.BenchmarkResult(log))

    except:
        print(f"...incomplete/bad result; skipping")


def average_result(attribute_name):
    return list(map(mean, zip(*(getattr(result, attribute_name) for result in results))))

def stdev_result(attribute_name):
    return list(map(stdev, zip(*(getattr(result, attribute_name) for result in results))))


n_records = average_result('n_records')
key_size = average_result('key_size')
value_size = average_result('value_size')
thruput_k_load = average_result('thruput_k_load')
thruput_k_c = average_result('thruput_k_c')
chi = average_result('chi')
mem_used = average_result('mem_used')
cpu_util = average_result('cpu_util')

if len(results) > 1:
    thruput_k_c_stdev = stdev_result('thruput_k_c')
    thruput_k_c_stdev_pct = [s/m for s,m in zip(thruput_k_c, thruput_k_c_stdev)]
    print("σ=", thruput_k_c_stdev)
    print("σ(%)=", thruput_k_c_stdev_pct)

print(n_records)
print(thruput_k_load)
print(thruput_k_c)

print(chi)
print(mem_used)
print(cpu_util)


fig, ax = plt.subplots()

ax.set_title("χ effect on Load, Point Query Throughput\n"
             f"(N={n_records[0]/1e6}M, k:v={key_size[0]}:{value_size[0]})")

series = []

n_points = min(len(chi), len(thruput_k_load), len(thruput_k_c), len(mem_used))

series += ax.plot(chi[:n_points], thruput_k_load[:n_points],
                  label='Load KOps/Second')

series += ax.plot(chi[:n_points], thruput_k_c[:n_points],
                  label='Point Query KOps/Second')

ax.set_xlabel("Checkpoint Distance (χ)")
ax.set_ylabel("KOps/Second")
ax.set_xscale("log")
#ax.set_yscale("log")
ax.set_ylim(0, 2400)
ax.grid()
ax.grid(which="minor", color="0.9")

ax2 = ax.twinx()

mem_used_gb = [n_bytes / 2**30 for n_bytes in mem_used]

series += ax2.plot(chi[:n_points], mem_used_gb[:n_points],
                   label='Memory Usage', color='green')

ax2.set_ylim(0, 80)
ax2.set_ylabel("Memory Usage (GB)")

ax.legend(series, [s.get_label() for s in series], loc='upper left')

plt.savefig("chi_no_read_impact.png")

