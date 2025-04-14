import os
import re
import sys
import matplotlib.pyplot as plt
import numpy as np


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


    
    
class ChiScalingResult:
    def __init__(self, log):
        self.log = log
        self.n_records = []
        self.key_size = []
        self.value_size = []
        self.chi = []
        self.thruput_k = []
        self.write_amp = []
        self.page_faults = []
        self.mem_used = []
        self.cpu_util = []
        self.cache_miss = []
        self.cache_ref = []
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
        parse_val(line, r'^# turtlekv  key_size_hint ==', int, -1, self.key_size)
        parse_val(line, r'^# turtlekv  value_size_hint ==', int, -1, self.value_size)
        parse_val(line, r'^# turtlekv  checkpoint_distance ==', int, -1, self.chi)
        parse_val(line, r'^\[ycsbc output\]	turtlekv	workloads/load.spec', float, -1, self.thruput_k)
        parse_val(line, r'^write_amplification', float, -1, self.write_amp)
        parse_val(line, r'^perf_stat.page-faults', int, -1, self.page_faults)
        parse_val(line, r'.*Actual memory used.*', int, 2, self.mem_used)
        parse_val(line, r'^cpu_utilization', float, -1, self.cpu_util)
        parse_val(line, r'^perf_stat.cache-misses', int, -1, self.cache_miss)
        parse_val(line, r'^perf_stat.cache-references', int, -1, self.cache_ref)

    def _image_name(self, suffix):
        name = self.log
        if name.endswith('.log'):
            name = name[:-4]
        name += suffix
        return name

    def _new_plot(self):
        plt.close()
        self.fig, self.ax = plt.subplots()
        self.series = []

    def _plot_tp_and_wa_vs_chi(self):
        self.ax.set_title(f"χ-scaling for N={self.n_records[0]} (key={self.key_size[0]}, value={self.value_size[0]})")
        
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
        print(f"Writing {image_name}...")
        plt.savefig(image_name)
                
    def plot_page_faults(self):
        self._new_plot()
        self._plot_tp_and_wa_vs_chi()
        
        self.ax2 = self.ax.twinx()
        self.series += self.ax2.plot(self.chi, self.page_faults, color='green', label='Page Faults')
        self.ax2.set_ylim(0, 1.6e7)

        self._write_image('_page-faults.png')

    def plot_mem_used(self):
        self._new_plot()
        self._plot_tp_and_wa_vs_chi()

        percent_faster = [100-self.thruput_k[0]*100/y for y in self.thruput_k]
        mb_used = [bytes_used / 2**20 for bytes_used in self.mem_used]

        speedup_per = [10*(used1 - used0) / (faster1 - faster0)
                       for (faster0, faster1, used0, used1)
                       in zip(percent_faster[:5], percent_faster[1:6], mb_used[:5], mb_used[1:6])]
        
        speedup_per += [10*(used1 - used0) / (faster1 - faster0)
                        for (faster0, faster1, used0, used1)
                        in zip(percent_faster[:-5], percent_faster[5:], mb_used[:-5], mb_used[5:])]

        
        
        print(speedup_per)
        
        
        self.ax2 = self.ax.twinx()

        self.series += self.ax2.plot(self.chi, mb_used,
                                     color='green', label='Memory Used (mb)')
        
        self.series += self.ax2.plot(self.chi, speedup_per,
                                     color='red', label="100KB per % Speedup")
        
        self.ax2.set_ylim(0, 65536)

        self._write_image('_mem-used.png')

    def plot_cpu_util(self):
        self._new_plot()
        self._plot_tp_and_wa_vs_chi()

        self.ax2 = self.ax.twinx()
        self.series += self.ax2.plot(self.chi, self.cpu_util,
                                     color='green', label='Cpu Utilization (%)')
        self.ax2.set_ylim(0, 1600)

        self._write_image('_cpu-util.png')

    def plot_cache_miss_rate(self):
        self._new_plot()
        self._plot_tp_and_wa_vs_chi()

        self.ax2 = self.ax.twinx()
        self.series += self.ax2.plot(self.chi,
                                     [miss * 100 / ref for (miss, ref) in zip(self.cache_miss, self.cache_ref)],
                                     color='green', label='Cache Miss Rate (%)')
        self.ax2.set_ylim(0, 100)

        self._write_image('_cache-miss-rate.png')        
        
    
    
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
