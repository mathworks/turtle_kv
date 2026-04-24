## Crash in benchmark test

Seems 100% reproducible.

```shell
T=32 CHI=160 WAL_MB=8192 CACHE_MB=16384 VL=256 N=5000000 cor test --build-type=Release --filter=*Benchmark*Get*T
```

Output:

```
[ RUN      ] BenchmarksTest.CheckpointGetThreads
WARNING: Logging before InitGoogleLogging() is written to STDERR
I20260424 09:01:10.055218 140630922682752 benchmarks.test.cpp:63] loading data file...
I20260424 09:01:10.729634 140630922682752 benchmarks.test.cpp:71] data file loaded; data_str.size() == 1073741824
I20260424 09:01:10.729665 140630922682752 benchmarks.test.cpp:96] wal=8GiB cache=16GiB chi=160 config.tree_options == TreeOptions{.node_size=4096, .leaf_size=2097152, .filter_bits_per_key=12, .filter_page_size=16384, .max_item_size=585,}
I20260424 09:01:10.744792 140630922682752 node_page_view.cpp:28] Registering page layout: kv_node_
I20260424 09:01:10.744802 140630922682752 leaf_page_view.cpp:30] Registering page layout: kv_leaf_; TURTLE_KV_PACK_KEYS_TOGETHER == 0
I20260424 09:01:10.745833 140630922682752 env_param.hpp:13] turtlekv_memtable_cache_alloc_log=1
I20260424 09:01:10.745838 140630922682752 env_param.hpp:13] turtlekv_memtable_cache_alloc_art=1
I20260424 09:01:11.410945 140627052176960 page_buffer.cpp:51] LLFS_ENABLE_PAGE_BUFFER_POOL=1                                                                                                                                                           I20260424 09:01:11.468722 140627043784256 volume.cpp:28] LLFS_WRITE_NEW_PAGES_ASAP=1                                                                                                                                                                   E20260424 09:01:12.213724 140627052176960 packed_leaf_page.hpp:107] FATAL: /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/src/turtle_kv/tree/packed_leaf_page.hpp:107: Assertion failed: this->magic == Packe
dLeafPage::kMagic
 (in `void turtle_kv::PackedLeafPage::check_magic() const`)

  this->magic == 0

  PackedLeafPage::kMagic == 1483478135718155971

E20260424 09:01:12.213750 140627052176960 assert_impl.hpp:19]

```

_(raw stack frames omitted)_

```
 0# batt::print_stack_trace() in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test                                                                                              1# batt::fail_check_exit() in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test                                                                                                2# turtle_kv::PackedLeafPage::check_magic() const in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test                                                                         3# turtle_kv::find_key_lower_bound_index(llfs::PageId, turtle_kv::KeyQuery&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test                                              4# turtle_kv::SegmentAlgorithms<turtle_kv::InMemoryNode::UpdateBuffer::Segment>::drop_pivot_range(batt::BasicInterval<batt::IClosedOpen<int, int> > const&, batt::BasicInterval<batt::IClosedOpen<std::basic_string_view<char, std::char_traits<char> >, std::basic_string_view<char, std::char_traits<char> > > > const&, llfs::BasicPageLoader<llfs::PinnedPage>&, turtle_kv::TreeOptions const&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
 5# turtle_kv::InMemoryNode::UpdateBuffer::SegmentedLevel::drop_pivot_range(batt::BasicInterval<batt::IClosedOpen<int, int> > const&, batt::BasicInterval<batt::IClosedOpen<std::basic_string_view<char, std::char_traits<char> >, std::basic_string_vi
ew<char, std::char_traits<char> > > > const&, llfs::BasicPageLoader<llfs::PinnedPage>&, turtle_kv::TreeOptions const&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
 6# turtle_kv::InMemoryNode::UpdateBuffer::SegmentedLevel::drop_after_pivot(int, std::basic_string_view<char, std::char_traits<char> > const&, llfs::BasicPageLoader<llfs::PinnedPage>&, turtle_kv::TreeOptions const&) in /mnt/optane905p_960_1/worksp
ace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
 7# void turtle_kv::NodeAlgorithms<turtle_kv::InMemoryNode>::split_level<turtle_kv::InMemoryNode::UpdateBuffer::SegmentedLevel, std::variant<turtle_kv::InMemoryNode::UpdateBuffer::EmptyLevel, turtle_kv::InMemoryNode::UpdateBuffer::MergedLevel, tur
tle_kv::InMemoryNode::UpdateBuffer::SegmentedLevel>, llfs::BasicPageLoader<llfs::PinnedPage> >(turtle_kv::InMemoryNode::UpdateBuffer::SegmentedLevel const&, int, std::variant<turtle_kv::InMemoryNode::UpdateBuffer::EmptyLevel, turtle_kv::InMemoryNo
de::UpdateBuffer::MergedLevel, turtle_kv::InMemoryNode::UpdateBuffer::SegmentedLevel>&, std::variant<turtle_kv::InMemoryNode::UpdateBuffer::EmptyLevel, turtle_kv::InMemoryNode::UpdateBuffer::MergedLevel, turtle_kv::InMemoryNode::UpdateBuffer::Segm
entedLevel>&, llfs::BasicPageLoader<llfs::PinnedPage>&, turtle_kv::TreeOptions const&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
 8# turtle_kv::InMemoryNode::try_split_direct(turtle_kv::BatchUpdateContext&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
 9# turtle_kv::InMemoryNode::try_split(turtle_kv::BatchUpdateContext&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
10# turtle_kv::Subtree::try_split(turtle_kv::BatchUpdateContext&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
11# turtle_kv::InMemoryNode::split_child(turtle_kv::BatchUpdateContext&, int) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
12# turtle_kv::InMemoryNode::make_child_viable(turtle_kv::BatchUpdateContext&, int) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
13# turtle_kv::InMemoryNode::flush_to_pivot(turtle_kv::BatchUpdateContext&, int) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
14# turtle_kv::InMemoryNode::flush_if_necessary(turtle_kv::BatchUpdateContext&, bool) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
15# turtle_kv::InMemoryNode::apply_batch_update(turtle_kv::BatchUpdate&, std::basic_string_view<char, std::char_traits<char> > const&, batt::StrongType<bool, turtle_kv::IsRoot_TAG>) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test                                                                                                                                                                                            16# turtle_kv::Subtree::apply_batch_update(turtle_kv::TreeOptions const&, batt::StrongType<int, turtle_kv::ParentNodeHeight_TAG>, turtle_kv::BatchUpdate&, std::basic_string_view<char, std::char_traits<char> > const&, batt::StrongType<bool, turtle_
kv::IsRoot_TAG>) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
17# turtle_kv::Checkpoint::apply_batch(batt::WorkerPool&, llfs::PageCacheJob&, turtle_kv::TreeOptions const&, turtle_kv::BatchUpdateMetrics&, llfs::PageCacheOvercommit&, std::unique_ptr<turtle_kv::DeltaBatch, std::default_delete<turtle_kv::DeltaBa
tch> >&&, batt::CancelToken const&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
18# turtle_kv::CheckpointGenerator::apply_batch(std::unique_ptr<turtle_kv::DeltaBatch, std::default_delete<turtle_kv::DeltaBatch> >&&, llfs::PageCacheOvercommit&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scal
ability/build/Release/src/turtle_kv_Test
19# turtle_kv::KVStore::apply_batch_to_checkpoint(std::unique_ptr<turtle_kv::DeltaBatch, std::default_delete<turtle_kv::DeltaBatch> >&&) in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/
turtle_kv_Test
20# turtle_kv::KVStore::checkpoint_update_thread_main()::{lambda()#1}::operator()() const in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
21# turtle_kv::KVStore::checkpoint_update_thread_main() in /mnt/optane905p_960_1/workspace/post-vldb-revision-upstream/turtle_kv.thread_scalability/build/Release/src/turtle_kv_Test
22# 0x00000000000E62B3 in /lib/x86_64-linux-gnu/libstdc++.so.6
23# start_thread at ./nptl/pthread_create.c:442
24# __GI___clone3 at ../sysdeps/unix/sysv/linux/x86_64/clone3.S:83

 0. InMemoryNode::apply_batch_update
    at turtle_kv/tree/in_memory_node.cpp:197
    in turtle_kv::InMemoryNode::apply_batch_update(turtle_kv::BatchUpdate&, const llfs::KeyView&, turtle_kv::IsRoot)::<lambda(std::ostream&, const void*)>
    stack offset: --
FATAL: signal 6 (Aborted):
[[raw stack]]
```
