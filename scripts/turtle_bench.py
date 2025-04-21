import os
import re
import sys
import matplotlib.pyplot as plt
import numpy as np


#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+--------------

KB_PER_SPEEDUP_PCT = False

#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+--------------

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


#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+--------------

class BenchmarkResult:
    def __init__(self, log):
        self.log = log

        # Input key params
        #
        self.n_records = []
        self.n_unique = []
        
        self.key_size = []
        self.value_size = []
        
        self.node_size = []
        self.leaf_size = []
        self.filter_size = []
        self.trie_size = []        

        self.node_cache = []
        self.leaf_cache = []
        self.filter_cache = []
        self.trie_cache = []

        self.buffer_level_trim = []
        self.chi = []
        self.cp_pipeline = []
        self.disk_path = []
        self.filter_bits = []
        self.max_flush = []
        self.min_flush = []
        self.storage_size = []
        self.wal_size = []

        # Output values
        #
        self.thruput_k_load = []
        self.thruput_k_a = []
        self.thruput_k_b = []
        self.thruput_k_c = []
        self.thruput_k_d = []
        self.thruput_k_e = []
        self.thruput_k_f = []
        self.write_amp = []
        self.page_faults = []
        self.mem_used = []
        self.cpu_util = []
        self.cache_miss = []
        self.cache_ref = []
        self.max_thread_cache = []
        self.mem_release_rate = []

        self.leaf_hit_rate = []
        self.node_hit_rate = []
        self.filter_hit_rate = []
        
        self.fig = None
        self.ax = None
        self.ax2 = None
        self.series = None

        with open(log, 'r') as fd:
            for line in fd:
                line = line.strip()
                self.visit(line)

    def get_input_count(self):
        return min(
            len(self.n_records),
            len(self.n_unique),
            len(self.key_size),
            len(self.value_size),
            len(self.leaf_size),
            len(self.chi),
            len(self.leaf_cache),
            len(self.cp_pipeline),
        )

    def get_input_key(self, i):
        return (
            self.n_records[i],
            self.n_unique[i],
            self.key_size[i],
            self.value_size[i],
            self.leaf_size[i],
            self.chi[i],
            self.leaf_cache[i],
            self.cp_pipeline[i],
        )

    def get_output_values(self, i):
        return (
            self.thruput_k_load[i],
            self.write_amp[i],
            self.page_faults[i],
            self.mem_used[i],
            self.cpu_util[i],
            self.cache_miss[i],
            self.cache_ref[i],
            self.max_thread_cache[i],
            self.mem_release_rate[i],
        )

    def visit(self, line):
        parse_val(line, r'^# Loading records:', float, -1, self.n_records)
        parse_val(line, r'.* n_puts == ', int, -1, self.n_records)
        parse_val(line, r'.* n_unique == ', int, -1, self.n_unique)

        parse_val(line, r'^# turtlekv  buffer_level_trim ==', int, -1, self.buffer_level_trim)
        parse_val(line, r'^# turtlekv  checkpoint_distance ==', int, -1, self.chi)
        parse_val(line, r'^# turtlekv  checkpoint_pipeline ==', int, -1, self.cp_pipeline)
        parse_val(line, r'^# turtlekv  disk_path ==', str, -1, self.disk_path)
        parse_val(line, r'^# turtlekv  filter_bits ==', int, -1, self.filter_bits)
        parse_val(line, r'^# turtlekv  filter_cache_size ==', int, -2, self.filter_cache)
        parse_val(line, r'^# turtlekv  filter_page_size ==', int, -1, self.filter_size)
        parse_val(line, r'^# turtlekv  key_size_hint ==', int, -1, self.key_size)
        parse_val(line, r'^# turtlekv  leaf_cache_size ==', int, -2, self.leaf_cache)
        parse_val(line, r'^# turtlekv  leaf_size ==', int, -2, self.leaf_size)
        parse_val(line, r'^# turtlekv  max_flush_factor ==', int, -1, self.max_flush)
        parse_val(line, r'^# turtlekv  min_flush_factor ==', int, -1, self.min_flush)
        parse_val(line, r'^# turtlekv  node_cache_size ==', int, -2, self.node_cache)
        parse_val(line, r'^# turtlekv  node_size ==', int, -2, self.node_size)
        parse_val(line, r'^# turtlekv  storage_size ==', int, -2, self.storage_size)
        parse_val(line, r'^# turtlekv  trie_index_cache_size ==', int, -2, self.trie_cache)
        parse_val(line, r'^# turtlekv  trie_index_reserve_size ==', int, -1, self.trie_size)
        parse_val(line, r'^# turtlekv  value_size_hint ==', int, -1, self.value_size)
        parse_val(line, r'^# turtlekv  wal_size ==', int, -2, self.wal_size)

        if "turtle_kv_bench: Killed" in line:
            self.thruput_k_load += [1]
            self.mem_used += [1]
        else:
            parse_val(line, r'^\[ycsbc output\].*turtlekv.*workloads/load.spec', float, -1, self.thruput_k_load)
            parse_val(line, r'.*Actual memory used.*', int, 2, self.mem_used)
            
        parse_val(line, r'^\[ycsbc output\].*turtlekv.*workloads/workloada.spec', float, -1, self.thruput_k_a)
        parse_val(line, r'^\[ycsbc output\].*turtlekv.*workloads/workloadb.spec', float, -1, self.thruput_k_b)
        parse_val(line, r'^\[ycsbc output\].*turtlekv.*workloads/workloadc.spec', float, -1, self.thruput_k_c)
        parse_val(line, r'^\[ycsbc output\].*turtlekv.*workloads/workloadd.spec', float, -1, self.thruput_k_d)
        parse_val(line, r'^\[ycsbc output\].*turtlekv.*workloads/workloade.spec', float, -1, self.thruput_k_e)
        parse_val(line, r'^\[ycsbc output\].*turtlekv.*workloads/workloadf.spec', float, -1, self.thruput_k_f)
        
        parse_val(line, r'^write_amplification', float, -1, self.write_amp)
        parse_val(line, r'^perf_stat.page-faults', int, -1, self.page_faults)
        parse_val(line, r'^cpu_utilization', float, -1, self.cpu_util)
        parse_val(line, r'^perf_stat.cache-misses', int, -1, self.cache_miss)
        parse_val(line, r'^perf_stat.cache-references', int, -1, self.cache_ref)
        parse_val(line, r'.*tcmalloc.max_total_thread_cache_bytes=.*', split_kv(int), -1, self.max_thread_cache)
        parse_val(line, r'.*tcmalloc.max_total_thread_cache_bytes ', int, -1, self.max_thread_cache)
        parse_val(line, r'.*\(tcmalloc\) MemoryReleaseRate=.*', split_kv(float), -1, self.mem_release_rate)
        parse_val(line, r'.*tcmalloc.memory_release_rate ', float, -1, self.mem_release_rate)

        parse_val(line, r'.*leaf_cache.hit_rate()', float, -1, self.leaf_hit_rate)
        parse_val(line, r'.*node_cache.hit_rate()', float, -1, self.node_hit_rate)
        parse_val(line, r'.*filter_cache.hit_rate()', float, -1, self.filter_hit_rate)
        
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

        self.series += self.ax.plot(self.chi, [100-self.thruput_k_load[0]*100/y for y in self.thruput_k_load],
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
        self.ax2.set_ylim(0, 5e7)

        self._write_image('page-faults')

    def plot_mem_used(self):
        self._new_plot()
        self._plot_tp_and_wa_vs_chi()

        percent_faster = [100-self.thruput_k_load[0]*100/y for y in self.thruput_k_load]
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
        self.ax2.set_ylim(0, 30)

        self._write_image('cache-miss-rate')
