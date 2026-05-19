#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the TurtleKV Project, under Apache License v2.0.
# See https:#www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

import math
import random
import shlex
import sys
import textwrap

from contextlib import contextmanager
from pathlib import Path
from random import Random

import yaml

COUNT_UNIT = 10 * 1000

MIN_THREADS = 2
MAX_THREADS = 8
MAX_OPS = COUNT_UNIT * 500

# For each block type, a list of the legal commands.
#
ALLOWED_COMMANDS = {
    "script": [
        "concurrent",
        "insert",
        "interleave",
        "parallel",
        "point_query",
        "update",
    ],
    "concurrent": [
        "insert",
        "interleave",
        "parallel",
        "point_query",
        "sequence",
        "update",        
    ],
    "interleave": [
        "insert",
        "point_query",
        "sequence",
        "update",
    ],
    "parallel": [
        "concurrent",
        "insert",
        "interleave",
        "point_query",
        "update",
    ],
    "sequence": [
        "insert",
        "interleave",
        "point_query",
        "update",        
    ],
}


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#
def main() -> int:
    """Generate random test scripts"""
    phrase = shlex.join(sys.argv)
    print(phrase)

    output_dir = sys.argv[1]
    seed_count = int(sys.argv[2])
    
    print(f'output dir: {output_dir}')
    print(f'seed count: {seed_count}')

    seeds = list(range(seed_count))
    
    for seed in seeds:
        script = RandomScript(random_seed=seed)
        file_path = Path(output_dir) / script._file_name
        print(file_path)

        with open(file_path, 'wt') as file_out:
            yaml.dump(script._tree, file_out)
    
    return 0


#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#
class RandomScript:
    def __init__(self, *, random_seed=17171977):
        self._rng = Random(random_seed)
        self._file_name = f"random_seed-0x{random_seed:08x}.generated.yml"
        self._config = None
        self._n_ops_remaining = 500 * COUNT_UNIT
        self._n_inserted = 0
        self._n_deferred_insertions = 0
        self._defer_count = 0
        self._depth = 0
        self._most_recent_command = None
        self._tree = { 'script': [] }
        self._block_stack = [('script', self._tree['script'])]
        # ----- --- -- -  -  -   -
        self.generate()

        assert self._n_ops_remaining == 0
        
    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def generate(self):
        self._tree["script"] = [
            self._generate_config(),
            { "create": { "remove_existing": True }, },
            { "open": {}, },            
        ] + self._generate_block(min_len=None)

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_config(self):
        assert self._config is None
        self._config = {
            "initial_capacity_gb": 0,
            "max_capacity_gb": 8,
            "wal_size_mb": random.randint(1, 256)*16,
            "node_size_kb": 4,
            "leaf_size_kb": int(2**random.randint(9, 13)),
            "key_size_hint": random.randint(8, 32),
            "value_size_hint": random.randint(1, 24)*10,
            "chi": random.randint(1, 999),
        }
        return { "config": self._config }

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _has_more_ops(self):
        return self._n_ops_remaining > 0

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _pick_n_ops(self):
        n = COUNT_UNIT * self._rng.randint(1, int(self._n_ops_remaining / COUNT_UNIT))
        self._n_ops_remaining -= n
        return n

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _top_block_name(self):
        return self._block_stack[-1][0]
    
    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _add_insert_key_dist(self, params):
        params['key_dist'] = self._rng.choice([
            'uniform'
        ])
        match params['key_dist']:
            case 'uniform':
                params['key_size'] = self._config['key_size_hint']

            case _:
                assert False  # bad key_dist

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _add_query_key_dist(self, params):
        params['key_dist'] = self._rng.choice([
            'zipf'
        ])
        match params['key_dist']:
            case 'zipf':
                params['random_seed'] = self._rng.randint(0, int(2**32-1))
                params['zipf_alpha'] = self._rng.uniform(0.5, 1.0)

            case _:
                assert False  # bad key_dist

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_block(self, *, min_len=1, max_len=None):
        block = []
        mean = 1.5
        block_len = None if min_len is None else (min_len + int(round(self._rng.expovariate(1.0 / mean))))

        while self._has_more_ops() and (block_len is None or
                                        (len(block) < block_len and
                                         (max_len is None or len(block) <= max_len))):
            next_command = self._generate_command()
            if len(block) > 0 and self._bad_sequence(block[-1], next_command):
                continue
            block.append(next_command)

        return block

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _bad_sequence(self, first, second):
        if 'insert' in first and 'insert' in second:
            return True
        return False

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_command(self):
        top_block_name = self._top_block_name()
        command = None
        
        while command is None:
            command_name = self._rng.choice(ALLOWED_COMMANDS[top_block_name])

            if self._n_inserted == 0 and command_name in {'concurrent', 'update', 'point_query'}:
                continue
            
            match command_name:
                case "concurrent":
                    command = self._generate_concurrent()

                case "insert":
                    command = self._generate_insert()

                case "interleave":
                    command = self._generate_interleave()

                case "parallel":
                    command = self._generate_parallel()

                case "point_query":
                    command = self._generate_point_query()

                case "sequence":
                    command = self._generate_sequence()

                case "update":
                    command = self._generate_update()

                case _:
                    assert False  # bad command name
        
        self._most_recent_command = command_name

        return command

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    @contextmanager
    def _scoped_block(self, name, command):
        self._block_stack.append((name, command))
        if name == 'interleave':
            self._defer_count += 1
        try:
            yield command
        finally:
            self._block_stack.pop()
            if name == 'interleave':
                self._defer_count -= 1
                if self._defer_count == 0:
                    self._n_inserted += self._n_deferred_insertions
                    self._n_deferred_insertions = 0
        
    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_concurrent(self):
        with self._scoped_block("concurrent", {"tasks": []}) as block:
            block["tasks"] += self._generate_block(min_len=2)
            return { "concurrent": block }

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_interleave(self):
        with self._scoped_block("interleave", []) as block:
            block += self._generate_block(min_len=2)
            return { "interleave": block }

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_parallel(self):
        params = {
            "n_threads": self._rng.randint(MIN_THREADS, MAX_THREADS),
            "stages": []
        }
        with self._scoped_block("parallel", params) as block:
            block["stages"] += self._generate_block(min_len=1)
            return { "parallel": block }

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_sequence(self):
        with self._scoped_block("sequence", []) as block:
            block += self._generate_block(min_len=2)
            return { "sequence": block }

    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_insert(self):
        params = {
            "count": self._pick_n_ops(),
            "value_size": self._config['value_size_hint'],
        }
        self._add_insert_key_dist(params)
        if self._defer_count > 0:
            self._n_deferred_insertions += params['count']
        else:
            self._n_inserted += params['count']
        return { "insert": params }
            
    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_update(self):
        params = {
            "count": self._pick_n_ops(),            
            "value_size": self._config['value_size_hint'],
        }
        self._add_query_key_dist(params)
        return { "update": params }
            
    #+++++++++++-+-+--+----- --- -- -  -  -   -
    def _generate_point_query(self):
        params = {
            "count": self._pick_n_ops(),            
        }
        self._add_query_key_dist(params)
        return { "point_query": params }
               

#=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

if __name__ == '__main__':
    sys.exit(main()) 
