"""
nbd_slow_filter_page_io.py - simulates slow filter page I/O using nbdkit.

Setup steps:

1. Install required packages:
```
sudo apt update && sudo apt install libnbd-bin nbd-server nbd-client nbdkit
```

2. Activate nbd kernel module:
```
sudo modprobe nbd
```

3. Create mount point:
```
sudo mkdir -p /mnt/turtlekv-nbd
```

4. Start the nbd server with python driver via nbdkit (in a dedicated terminal):
```
nbdkit -f -v python turtle_kv/scripts/nbd_slow_filter_page_io.py size=24G wdelaysize=131072 wdelaytime=10ms
```
Verify in (main terminal):
```
nbdinfo --list nbd://localhost
```

5. Connect the block device:
```
sudo nbd-client localhost /dev/nbd0
lsblk | grep nbd0
```

6. Create a blank filesystem on the connected device:
```
sudo mkfs.ext4 /dev/nbd0
```

7. Mount the filesystem and set permissions
```
sudo mount /dev/nbd0 /mnt/turtlekv-nbd
df -h | grep -E 'Filesystem|/dev/nbd0'
sudo chmod 0777 -R /mnt/turtlekv-nbd/.
```

8. Run turtle_kv unit test:
```
export TURTLE_KV_TEST_DIR=/mnt/turtlekv-nbd
cd turtle_kv
cor test --filter=KVStoreTest.CreateAndOpen
```

9. Teardown:
```
sudo umount /mnt/turtlekv-nbd
sudo nbd-client -d /dev/nbd0
# (in the nbdkit server terminal, CTRL-C)
```
"""

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# See:
# - https://libguestfs.org/nbdkit-python-plugin.3.html
# - https://gitlab.com/nbdkit/nbdkit/-/tree/ca73317509658e3abd5cb1d535eca95bbdb2abbc/plugins/python/examples
#
# Based on:
# - https://gitlab.com/nbdkit/nbdkit/-/blob/ca73317509658e3abd5cb1d535eca95bbdb2abbc/plugins/python/examples/ramdisk.py
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

import errno
from threading import Lock
import nbdkit

API_VERSION = 2

KB = 1024
MB = KB * 1024
GB = MB * 1024
TB = GB * 1024

lock = Lock()
disk = None
size = 8 * GB
wdelaysize = None
wdelaytime = (0, 0) # (sec, nanosec)


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# Although Python code cannot be run in parallel, if your
# plugin callbacks sleep then you can improve parallelism
# by relaxing the thread model.
#
def thread_model():
    return nbdkit.THREAD_MODEL_PARALLEL


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#
def config(key, value):
    global size, wdelaysize, wdelaytime
    #----- --- -- -  -  -   -
    action = 'parsed'
    parsed = None

    if key == 'size':
        size = nbdkit.parse_size(value)
        parsed = size

    elif key == 'wdelaysize':
        wdelaysize = nbdkit.parse_size(value)
        parsed = wdelaysize

    elif key == 'wdelaytime':
        wdelaytime = nbdkit.parse_delay(key, value)
        parsed = wdelaytime

    else:
        action='ignored'

    nbdkit.debug("%s parameter %s=%s (parsed=%s)" % (action, key, value, parsed))


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#
def open(readonly):
    global disk, size
    #----- --- -- -  -  -   -
    nbdkit.debug("open: readonly=%d, tls=%r" % (readonly, nbdkit.is_tls()))
    with lock:
        disk = bytearray(size)


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#
def get_size(h):
    global size
    #----- --- -- -  -  -   -
    return size


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#
def pread(h, buf, offset, flags):
    global disk
    #----- --- -- -  -  -   -
    end = offset + len(buf)
    with lock:
        buf[:] = disk[offset:end]


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#
def pwrite(h, buf, offset, flags):
    global disk, size, wdelaysize, wdelaytime
    #----- --- -- -  -  -   -
    length = len(buf)

    if wdelaysize is not None and length == wdelaysize:
        nbdkit.debug("pwrite: delaying write of size %d by %ds+%dns" % (length, wdelaytime[0], wdelaytime[1]))
        nbdkit.nanosleep(*wdelaytime)

    end = offset + len(buf)
    with lock:
        disk[offset:end] = buf


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#
def zero(h, count, offset, flags):
    global disk
    #----- --- -- -  -  -   -
    if flags & nbdkit.FLAG_MAY_TRIM:
        with lock:
            disk[offset:offset+count] = bytearray(count)
    else:
        nbdkit.set_error(errno.EOPNOTSUPP)
        raise Exception
