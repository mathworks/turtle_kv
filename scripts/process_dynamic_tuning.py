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

def scale_rgb(scale=0.5):
    def impl(rgb):
        r = rgb[1:3]
        g = rgb[3:5]
        b = rgb[5:7]
        return f"#{int(int(r, 16)*scale):02x}{int(int(g, 16)*scale):02x}{int(int(b, 16)*scale):02x}"
    return impl

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
            result.chi_load,
            result.chi_a,
            result.chi_b,
            result.chi_c,
            result.chi_d,
            result.chi_f,
            map(int, result.thruput_k_load),
            map(int, result.thruput_k_a),
            map(int, result.thruput_k_b),
            map(int, result.thruput_k_c),
            map(int, result.thruput_k_d),
            map(int, result.thruput_k_f),
            fillvalue=0,
        ))

        combined = {
            tuple(vals[0:6]): vals[6:]
            for vals in combined
        }

        print("combined=")
        for c in combined.items():
            print(c)

        plt.rcParams['font.family'] = 'sans-serif'
        plt.rcParams['font.sans-serif'] = 'Helvetica'

        fig, ax = plt.subplots()

        fig.set_size_inches(5, 3.5)

        dynamic = combined[(384, 2, 1, 1, 4, 1)]
        print("dynamic=\n",dynamic)

        w = 0.1
        p = 0.02
        i = 0

        cm1 = [
            "#a6cee3",
            "#1f78b4",
            "#b2df8a",
            "#33a02c",
            "#fb9a99",
            "#e31a1c",
            "#fdbf6f",
        ]

        cm1_half = list(map(scale_rgb(0.5), cm1))

        cm2 = [
            "#8dd3c7",
            "#ffffb3",
            "#bebada",
            "#fb8072",
            "#80b1d3",
            "#fdb462",
            "#b3de69",
        ]

        cm3 = [
            "#e41a1c",
            "#377eb8",
            "#4daf4a",
            "#984ea3",
            "#ff7f00",
            "#ffff33",
            "#a65628",
        ]

        cm = cm1
        cm_edge = list(map(scale_rgb(0.7), cm))

        rects = ax.bar([i*(w+p) + x for x in range(6)], dynamic, width=w,
                       label="χ=384,2,1,1,4,1",
                       color=cm[i],
                       edgecolor=cm_edge[i],
                       linewidth=0.3)
        i += 1
        ax.bar_label(rects, padding=3)

        try:
            chi1 = combined[(1, 1, 1, 1, 1, 1)]
            rects = ax.bar([i*(w+p) + x for x in range(6)], chi1, width=w,
                           label="χ=1",
                           color=cm[i],
                           edgecolor=cm_edge[i],
                           linewidth=0.3)
        except:
            print("chi=1 not found")

        i += 1

        chi2 = combined[(2, 2, 2, 2, 2, 2)]
        rects = ax.bar([i*(w+p) + x for x in range(6)], chi2, width=w,
                       label="χ=2",
                       color=cm[i],
                       edgecolor=cm_edge[i],
                       linewidth=0.3)

        i += 1

        chi16 = combined[(16, 16, 16, 16, 16, 16)]
        rects = ax.bar([i*(w+p) + x for x in range(6)], chi16, width=w,
                       label="χ=16",
                       color=cm[i],
                       edgecolor=cm_edge[i],
                       linewidth=0.3,
                       tick_label=['Load\nrandom',
                                   'A (50/50)\nzipf',
                                   'B (95/5)\nzipf',
                                   'C (100/0)\nzipf',
                                   'D (95/5)\nlatest',
                                   'F (50/50)\nr/m/w'])
        i += 1

        chi32 = combined[(32, 32, 32, 32, 32, 32)]
        rects = ax.bar([i*(w+p) + x for x in range(6)], chi32, width=w,
                       label="χ=32",
                       color=cm[i],
                       edgecolor=cm_edge[i],
                       linewidth=0.3)
        i += 1

        try:
            chi256 = combined[(256, 256, 256, 256, 256, 256)]
            rects = ax.bar([i*(w+p) + x for x in range(6)], chi256, width=w,
                           label="χ=256",
                           color=cm[i],
                           edgecolor=cm_edge[i],
                           linewidth=0.3)
        except:
            print("chi=256 not found")

        i += 1

        try:
            chi512 = combined[(512, 512, 512, 512, 512, 512)]
            rects = ax.bar([i*(w+p) + x for x in range(6)], chi512, width=w,
                           label="χ=512",
                           color=cm[i],
                           edgecolor=cm_edge[i],
                           linewidth=0.3)
        except:
            print("chi=512 not found")

        i += 1

        #ax.bar_label(rects, padding=3)

        ax.set_ylim(0, 9999)
        ax.set_xlabel("Workload")
        ax.set_ylabel("KOps/Second")
        ax.set_title("YCSB Dynamic vs Static χ-tuning\n"
                     "(N=700M, 32 threads, k:v=24:100)")
        ax.legend(fontsize="smaller")
        ax.set_axisbelow(True)
        ax.set_yticks([y*1000 for y in range(10)])
        ax.yaxis.grid(which="major", linewidth=0.5, alpha=0.5)
        ax.yaxis.grid(which="minor", linewidth=0.3, alpha=0.3)

        plt.tight_layout()
        plt.savefig(log + ".pdf")

    except:
        raise
        print(f"...incomplete/bad result; skipping")
