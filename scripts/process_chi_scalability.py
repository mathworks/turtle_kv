import os
import re
import sys
import matplotlib.pyplot as plt
import numpy as np


KB_PER_SPEEDUP_PCT = False


if len(sys.argv) < 2:
    print("Error: must provide at least one argument (the result log to process)",
          file=sys.stderr)
    sys.exit(1);


logs = sys.argv[1:]

def parse_val(line, pattern, val_type, pos, dst):
    parts = []
    try:
        if re.match(pattern, line):
            parts = [p for p in re.split(r'[ \t]', line) if p]
            value = val_type(parts[pos])
            dst.append(value)
    except:
        print(f"parts: {parts}")
        raise


def split_kv(val_type, sep='='):
    def impl(kv_str):
        key_str, val_str = kv_str.split(sep)
        return val_type(val_str)

    return impl


class ChiScalingResult:
    def __init__(self, log):
        self.log = log
        self.n_records = []
        self.n_unique = []
        self.key_size = []
        self.value_size = []
        self.leaf_size = []
        self.chi = []
        self.leaf_cache = []
        self.cp_pipeline = []
        self.thruput_k = []
        self.write_amp = []
        self.page_faults = []
        self.mem_used = []
        self.cpu_util = []
        self.cache_miss = []
        self.cache_ref = []
        self.max_thread_cache = []
        self.mem_release_rate = []
        self.fig = None
        self.ax = None
        self.ax2 = None
        self.series = None

        with open(log, 'r') as fd:
            for line in fd:
                line = line.strip()
                self.visit(line)

    def visit(self, line):
        parse_val(line, r'.* n_puts == ', int, -1, self.n_records)
        parse_val(line, r'.* n_unique == ', int, -1, self.n_unique)
        parse_val(line, r'^# turtlekv  key_size_hint ==', int, -1, self.key_size)
        parse_val(line, r'^# turtlekv  value_size_hint ==', int, -1, self.value_size)
        parse_val(line, r'^# turtlekv  checkpoint_distance ==', int, -1, self.chi)
        parse_val(line, r'^# turtlekv  leaf_cache_size ==', int, -2, self.leaf_cache)
        parse_val(line, r'^# turtlekv  leaf_size ==', int, -2, self.leaf_size)
        parse_val(line, r'^# turtlekv  checkpoint_pipeline ==', int, -1, self.cp_pipeline)
        parse_val(line, r'^\[ycsbc output\]	turtlekv	workloads/load.spec', float, -1, self.thruput_k)
        parse_val(line, r'^write_amplification', float, -1, self.write_amp)
        parse_val(line, r'^perf_stat.page-faults', int, -1, self.page_faults)
        parse_val(line, r'.*Actual memory used.*', int, 2, self.mem_used)
        parse_val(line, r'^cpu_utilization', float, -1, self.cpu_util)
        parse_val(line, r'^perf_stat.cache-misses', int, -1, self.cache_miss)
        parse_val(line, r'^perf_stat.cache-references', int, -1, self.cache_ref)
        parse_val(line, r'.*tcmalloc.max_total_thread_cache_bytes=.*', split_kv(int), -1, self.max_thread_cache)
        parse_val(line, r'.*tcmalloc.max_total_thread_cache_bytes ', int, -1, self.max_thread_cache)
        parse_val(line, r'.*\(tcmalloc\) MemoryReleaseRate=.*', split_kv(float), -1, self.mem_release_rate)
        parse_val(line, r'.*tcmalloc.memory_release_rate ', float, -1, self.mem_release_rate)

    def _new_plot(self):
        plt.close()
        self.fig, self.ax = plt.subplots()
        self.series = []

    def _plot_tp_and_wa_vs_chi(self):
        n_m = self.n_records[0]/1e6 if len(self.n_records) > 0 else 0
        n_unique_m = self.n_unique[0]/1e6 if len(self.n_unique) > 0 else n_m
        leaf_cache_mb = self.leaf_cache[0]/(2**20) if len(self.leaf_cache) > 0 else None
        cp_pipeline = self.cp_pipeline[0] if len(self.cp_pipeline) > 0 else None
        mem_cache_mb = self.max_thread_cache[0]/(2**20) if len(self.max_thread_cache) > 0 else None
        mem_release_rate = self.mem_release_rate[0] if len(self.mem_release_rate) > 0 else None
        leaf_size_mb = self.leaf_size[0]/(2**20) if len(self.leaf_size) > 0 else None

        self.ax.set_title(f"χ-scaling for N={n_m}M Unique={n_unique_m}M"
                          f" (k:v={self.key_size[0]}:{self.value_size[0]}, L={leaf_size_mb})"
                          f"\n(cache={leaf_cache_mb}mb "
                          f"tcmalloc={mem_cache_mb}mb,{mem_release_rate} "
                          f"pipeline={cp_pipeline})")

        self.series += self.ax.plot(self.chi, [100-self.thruput_k[0]*100/y for y in self.thruput_k],
                                    label='PUT latency')

        self.series += self.ax.plot(self.chi, [100-y*100/self.write_amp[0] for y in self.write_amp],
                                    label='Write Amplification')

        self.ax.set_xlabel('Checkpoint Distance (χ)')
        self.ax.set_ylabel('Improvement (%)')
        self.ax.set_xscale('log')
        self.ax.set_ylim(0, 100)
        self.ax.grid()
        self.ax.grid(which="minor", color="0.9")

    def _write_image(self, suffix):
        self.ax.legend(self.series, [s.get_label() for s in self.series], loc='upper left')    
        image_name = self._image_name(suffix)
        print(f"  Writing {image_name}...")
        plt.savefig(image_name)

    def _image_name(self, suffix):
        name = self.log
        if name.endswith('.log'):
            name = name[:-4]
        name += "." + suffix + ".png"
        return name

    def plot_page_faults(self):
        self._new_plot()
        self._plot_tp_and_wa_vs_chi()

        self.ax2 = self.ax.twinx()
        self.series += self.ax2.plot(self.chi, self.page_faults, color='green', label='Page Faults')
        self.ax2.set_ylim(0, 1.6e7)

        self._write_image('page-faults')

    def plot_mem_used(self):
        self._new_plot()
        self._plot_tp_and_wa_vs_chi()

        percent_faster = [100-self.thruput_k[0]*100/y for y in self.thruput_k]
        mb_used = [bytes_used / 2**20 for bytes_used in self.mem_used]

        self.ax2 = self.ax.twinx()

        self.series += self.ax2.plot(self.chi, mb_used,
                                     color='green', label='Memory Used (mb)')
        
        if KB_PER_SPEEDUP_PCT:
            speedup_per = [10*(used1 - used0) / (faster1 - faster0)
                           for (faster0, faster1, used0, used1)
                           in zip(percent_faster[:5], percent_faster[1:6], mb_used[:5], mb_used[1:6])]

            speedup_per += [10*(used1 - used0) / (faster1 - faster0)
                            for (faster0, faster1, used0, used1)
                            in zip(percent_faster[:-5], percent_faster[5:], mb_used[:-5], mb_used[5:])]

            self.series += self.ax2.plot(self.chi, speedup_per,
                                         color='red', label="100KB per % Speedup")

        self.ax2.set_ylim(0, 1e5)

        self._write_image('mem-used')

    def plot_cpu_util(self):
        self._new_plot()
        self._plot_tp_and_wa_vs_chi()

        self.ax2 = self.ax.twinx()
        self.series += self.ax2.plot(self.chi, self.cpu_util,
                                     color='green', label='CPU Utilization (%)')
        self.ax2.set_ylim(0, 1600)

        self._write_image('cpu-util')

    def plot_cache_miss_rate(self):
        self._new_plot()
        self._plot_tp_and_wa_vs_chi()

        self.ax2 = self.ax.twinx()
        self.series += self.ax2.plot(self.chi,
                                     [miss * 100 / ref for (miss, ref) in zip(self.cache_miss, self.cache_ref)],
                                     color='green', label='CPU Cache Miss Rate (%)')
        self.ax2.set_ylim(0, 100)

        self._write_image('cache-miss-rate')


#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+--------------

for log in logs:
    print(f"Processing {log}...")

    try:
        result = ChiScalingResult(log)
        result.plot_page_faults()
        result.plot_mem_used()
        result.plot_cpu_util()
        result.plot_cache_miss_rate()
    except:
        print(f"...incomplete/bad result; skipping")
