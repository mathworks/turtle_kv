# This script is a place to put information pertaining to Huge page support in linux.
#

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# Get general kernel memory configuration/status
#
cat /proc/meminfo

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# Check the page size being used by running program
#
grep KernelPageSize /proc/$(pgrep turtle_kv_bench)/smaps | sort | uniq

