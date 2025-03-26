#pragma once

#include <turtle_kv/import/seq.hpp>

#include <batteries/assert.hpp>
#include <batteries/stream_util.hpp>

#include <algorithm>

namespace turtle_kv {

template <typename SubTotals>
inline SplitParts split_parts(const SubTotals& sub_totals,
                              MinPartSize min_part_size,
                              MaxPartSize max_part_size,
                              MaxItemSize max_item_size)
{
  BATT_CHECK_GE(max_part_size, min_part_size);

  // Base case: empty sequence.
  //
  if (sub_totals.size() < 2) {
    return {{0}};
  }

  const usize total_size = sub_totals.back() - sub_totals.front();

  // Base case: all pivots will fit into a single part.
  //
  if (total_size <= max_part_size) {
    return {{0, sub_totals.size() - 1}};
  }

  const usize base_size = sub_totals.front();
  const auto first_item = std::next(sub_totals.begin());
  const auto last_item = sub_totals.end();

  const auto debug_info = [&](std::ostream& out) {
    out << std::endl;
    std::vector<usize> item_sizes;
    usize prev_sub_total = sub_totals.front();
    for (usize t : boost::make_iterator_range(first_item, last_item)) {
      item_sizes.emplace_back(t - prev_sub_total);
      prev_sub_total = t;
      if (item_sizes.back() > max_item_size) {
        LOG(WARNING) << "item[" << item_sizes.size() - 1
                     << "] size exceeds max: " << item_sizes.back() << " > " << max_item_size
                     << std::endl;
      }
    }
    out << BATT_INSPECT(min_part_size) << std::endl
        << BATT_INSPECT(max_part_size) << std::endl
        << BATT_INSPECT(max_item_size) << std::endl
        << "sub_totals=" << batt::dump_range(sub_totals, batt::Pretty::True) << std::endl
        << "item_sizes=" << batt::dump_range(item_sizes, batt::Pretty::True) << std::endl;
  };

  usize n_parts = 0;
  {
    // First pass: first-to-last.  Greedily assign as many items to each part as possible without
    // exceeding the max_part_size.
    //
    usize prev_sub_total = base_size;
    auto next_item = first_item;

    while (next_item != last_item) {
      const usize part_end_sub_total_target = prev_sub_total + max_part_size;

      // Start binary search right after next_item so we include at least one item per part.
      //
      next_item = std::upper_bound(std::next(next_item), last_item, part_end_sub_total_target);
      const usize part_end_sub_total = *std::prev(next_item);

      BATT_CHECK_GT(part_end_sub_total, prev_sub_total) << debug_info;

      const usize part_size = part_end_sub_total - prev_sub_total;
      BATT_CHECK_LE(part_size, max_part_size) << debug_info;

      prev_sub_total = part_end_sub_total;
      ++n_parts;
    }
  }

  SplitParts result;
  auto& parts = result.offsets;
  {
    // Second pass: allocate parts as equally as possible.
    //
    usize n_remaining = n_parts;
    usize prev_sub_total = base_size;
    usize total_remaining = total_size;
    const usize max_waste_per_part = max_item_size - 1;
    auto next_item = first_item;

    while (next_item != last_item) {
      const usize target_average = total_remaining / n_remaining;

      BATT_CHECK_LE(target_average, max_part_size) << debug_info;

      // Compute the minimum size we must pack into this part so that if all parts afterwards are
      // packed as inefficiently as possible, we will still succeed in generating only `n_parts`.
      //
      const usize part_size_lower_bound = std::clamp<usize>(
          total_remaining - (n_remaining - 1) * (max_part_size - max_waste_per_part),
          /*lower_bound=*/min_part_size,
          /*upper_bound=*/max_part_size);

      // We would prefer to pack an amount equal to the average across all parts, but this goal must
      // never be less than the lower bound we calculated above.
      //
      const usize target_part_size = std::max<usize>(part_size_lower_bound, target_average);
      const usize part_end_sub_total_target = prev_sub_total + target_part_size;

      // Start binary search right after next_item so we include at least one item per part.
      //
      auto part_end = std::upper_bound(std::next(next_item), last_item, part_end_sub_total_target);
      usize part_end_sub_total = *std::prev(part_end);
      usize part_size = part_end_sub_total - prev_sub_total;

      // Include one more when we are under the lower bound or when it is possible and beneficial to
      // do so.
      //
      if (part_end != last_item && part_size < target_part_size) {
        const bool too_small = part_size < part_size_lower_bound;
        const usize delta_without_next = target_part_size - part_size;
        const usize size_with_next = *part_end - prev_sub_total;
        const usize delta_with_next = (size_with_next < target_part_size)
                                          ? (target_part_size - size_with_next)
                                          : (size_with_next - target_part_size);

        if ((too_small || delta_with_next <= delta_without_next) &&
            (size_with_next <= max_part_size)) {
          ++part_end;
          part_end_sub_total = *std::prev(part_end);
          part_size = part_end_sub_total - prev_sub_total;
        }
      }
      BATT_CHECK_LE(part_size, max_part_size) << debug_info;

      const usize part_end_index = std::distance(first_item, part_end);
      BATT_CHECK_GT(part_end_index, parts.back()) << debug_info;

      parts.emplace_back(part_end_index);
      total_remaining -= part_size;
      prev_sub_total = part_end_sub_total;
      next_item = std::next(first_item, part_end_index);
      --n_remaining;
    }
  }

  BATT_CHECK_EQ(n_parts, parts.size() - 1) << debug_info;

  return result;
}

}  // namespace turtle_kv
