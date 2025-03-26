#pragma once

#include <turtle_kv/core/algo/decay_to_item.hpp>

#include <turtle_kv/util/flatten.hpp>

#include <batteries/async/slice_work.hpp>
#include <batteries/async/work_context.hpp>
#include <batteries/async/worker_pool.hpp>

namespace turtle_kv {

template <typename Src, typename Dst, typename GroupEq, typename CompactFn, bool kDecayValue>
Dst parallel_compact(batt::WorkerPool& worker_pool,
                     Src src_begin,
                     Src src_end,
                     const Dst& dst_begin,
                     const GroupEq& group_eq,
                     const CompactFn& compact_fn,
                     DecayToItem<kDecayValue> decay_to_item,
                     batt::TaskSize min_task_size,
                     batt::TaskCount max_tasks);

template <typename Src,
          typename Dst,
          typename ChunksOut,
          typename GroupEq,
          typename CompactFn,
          bool kDecayValue>
ChunksOut parallel_compact_into_chunks(batt::WorkerPool& worker_pool,
                                       Src src_begin,
                                       Src src_end,
                                       const Dst& dst_begin,
                                       ChunksOut chunks_begin,
                                       const GroupEq& group_eq,
                                       const CompactFn& compact_fn,
                                       DecayToItem<kDecayValue> decay_to_item,
                                       batt::TaskSize min_task_size,
                                       batt::TaskCount max_tasks);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
namespace detail {

template <typename GroupEq>
struct GroupEqBase {
  explicit GroupEqBase(const GroupEq& group_eq) : group_eq_{group_eq}
  {
  }

  GroupEq group_eq_;
};

template <typename GroupEq, typename CompactFn>
struct CompactShardBase : GroupEqBase<GroupEq> {
  explicit CompactShardBase(const GroupEq group_eq, const CompactFn& compact_fn)
      : GroupEqBase<GroupEq>{group_eq}
      , compact_fn_{compact_fn}
  {
  }

  CompactFn compact_fn_;
};

template <typename Src, typename Dst, typename GroupEq, typename CompactFn, typename Decay>
struct CompactShard : CompactShardBase<GroupEq, CompactFn> {
  explicit CompactShard(Src src_begin,
                        Src src_end,
                        Dst dst_begin,
                        const GroupEq& group_eq,
                        const CompactFn& compact_fn) noexcept
      : CompactShardBase<GroupEq, CompactFn>{group_eq, compact_fn}
      , src_begin_{src_begin}
      , src_end_{src_end}
      , dst_begin_{dst_begin}
      , dst_end_{dst_begin}
  {
  }

  void run_compact()
  {
    if (this->src_begin_ == this->src_end_) {
      return;
    }

    // Copy the first item explicitly to establish the loop invariant that `dst_end_` always points
    // to the current combine target.
    //
    *this->dst_begin_ = *this->src_begin_;
    ++this->src_begin_;

    for (; (this->src_begin_ != this->src_end_); ++this->src_begin_) {
      if (this->group_eq_(*this->dst_end_, *this->src_begin_)) {
        // Same group as the current one; either combine the two or throw out the second (which is
        // older).
        //
        if (!this->dst_end_->needs_combine()) {
          continue;
        }
        *this->dst_end_ = this->compact_fn_(*this->dst_end_, *this->src_begin_);
      } else {
        // New group; advance this->dst_end_.
        //
        if (Decay::keep_item(*this->dst_end_)) {
          ++this->dst_end_;
        }
        *this->dst_end_ = *this->src_begin_;
      }
    }

    // Now dst_end_ must advance past the last actual combined item.
    //
    if (Decay::keep_item(*this->dst_end_)) {
      ++this->dst_end_;
    }
  }

  Src src_begin_;
  Src src_end_;
  Dst dst_begin_;
  Dst dst_end_;
};

template <typename Src,
          typename Dst,
          typename GroupEq,
          typename CompactFn,
          typename Shards,
          bool kDecayValue>
void parallel_compact_impl(batt::WorkerPool& worker_pool,
                           Src src_begin,
                           Src src_end,
                           const Dst& dst_begin,
                           const GroupEq& group_eq,
                           const CompactFn& compact_fn,
                           Shards& shards,
                           DecayToItem<kDecayValue>,
                           batt::TaskSize min_task_size,
                           batt::TaskCount max_tasks)
{
  auto src_size = std::distance(src_begin, src_end);

  // Each task will compute a single shard; each shard lives in an independent vector.
  //
  const isize n_shards = std::min<isize>(max_tasks, (src_size + min_task_size - 1) / min_task_size);

  // Create a WorkContext to manage the fork/join of tasks.
  //
  batt::ScopedWorkContext context{worker_pool};

  // Build the shard ranges.
  //
  Dst dst_next = dst_begin;
  for (isize shard_i = 0; src_begin != src_end; ++shard_i) {
    usize this_shard_size = src_size / (n_shards - shard_i);
    Src shard_src_begin = src_begin;
    Src shard_src_end = std::next(shard_src_begin, this_shard_size);

    // Adjust the end of the shard src range to include an entire group.
    //  TODO [tastolfi 2021-07-30] In order to fully parallelize key-wise merge operators,
    //   we should instead allow groups to be split and then fix it at the end (taking advantage of
    //   the associativity of Edit ops).
    //
    if (shard_src_begin != shard_src_end) {
      Src shard_src_last = std::prev(shard_src_end);
      while (shard_src_end != src_end) {
        if (!group_eq(*shard_src_last, *shard_src_end)) {
          break;
        }
        shard_src_last = shard_src_end;
        ++shard_src_end;
        ++this_shard_size;
      }
    }

    Dst shard_dst_begin = dst_next;
    Dst shard_dst_end = std::next(shard_dst_begin, this_shard_size);

    shards.emplace_back(shard_src_begin, shard_src_end, shard_dst_begin, group_eq, compact_fn);

    dst_next = shard_dst_end;
    src_begin = shard_src_end;
    src_size -= this_shard_size;
  }

  if (shards.empty()) {
    return;
  }

  isize i = 0;
  for (auto& shard : shards) {
    auto work_fn = [p_shard = &shard] {
      (*p_shard)->run_compact();
    };
    ++i;
    if (i < n_shards) {
      BATT_CHECK_OK(context.async_run(work_fn)) << "worker_pool must not be closed!";
    } else {
      work_fn();
    }
  }
}

}  // namespace detail
   //
   //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

template <typename Src, typename Dst, typename GroupEq, typename CompactFn, bool kDecayValue>
Dst parallel_compact(batt::WorkerPool& worker_pool,
                     Src src_begin,
                     Src src_end,
                     const Dst& dst_begin,
                     const GroupEq& group_eq,
                     const CompactFn& compact_fn,
                     DecayToItem<kDecayValue> decay_to_item,
                     batt::TaskSize min_task_size,
                     batt::TaskCount max_tasks)
{
  batt::SmallVec<batt::CpuCacheLineIsolated<
                     detail::CompactShard<Src, Dst, GroupEq, CompactFn, DecayToItem<kDecayValue>>>,
                 64>
      shards;

  detail::parallel_compact_impl(worker_pool,
                                src_begin,
                                src_end,
                                dst_begin,
                                group_eq,
                                compact_fn,
                                shards,
                                decay_to_item,
                                min_task_size,
                                max_tasks);

  if (shards.empty()) {
    return dst_begin;
  }

  // Compact the destination range to remove empty spaces.
  //
  Dst min_dst_begin = shards.front()->dst_begin_;
  for (const auto& shard : shards) {
    if (shard->dst_begin_ != min_dst_begin) {
      min_dst_begin = std::copy(shard->dst_begin_, shard->dst_end_, min_dst_begin);
    } else {
      min_dst_begin = shard->dst_end_;
    }
  }

  return min_dst_begin;
}

template <typename Src,
          typename Dst,
          typename ChunksOut,
          typename GroupEq,
          typename CompactFn,
          bool kDecayValue>
ChunksOut parallel_compact_into_chunks(batt::WorkerPool& worker_pool,
                                       Src src_begin,
                                       Src src_end,
                                       const Dst& dst_begin,
                                       ChunksOut chunks_begin,
                                       const GroupEq& group_eq,
                                       const CompactFn& compact_fn,
                                       DecayToItem<kDecayValue> decay_to_item,
                                       batt::TaskSize min_task_size,
                                       batt::TaskCount max_tasks)
{
  batt::SmallVec<batt::CpuCacheLineIsolated<
                     detail::CompactShard<Src, Dst, GroupEq, CompactFn, DecayToItem<kDecayValue>>>,
                 64>
      shards;

  detail::parallel_compact_impl(worker_pool,
                                src_begin,
                                src_end,
                                dst_begin,
                                group_eq,
                                compact_fn,
                                shards,
                                decay_to_item,
                                min_task_size,
                                max_tasks);
  isize offset = 0;
  Dst dst_end = dst_begin;

  for (const auto& shard : shards) {
    dst_end = shard->dst_end_;
    *chunks_begin =
        Chunk<Dst>{offset, boost::make_iterator_range(shard->dst_begin_, shard->dst_end_)};
    offset += chunks_begin->items.size();
    ++chunks_begin;
  }

  *chunks_begin = make_end_chunk(offset, dst_end);
  return std::next(chunks_begin);
}

}  // namespace turtle_kv
