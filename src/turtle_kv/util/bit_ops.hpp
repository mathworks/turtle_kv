#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>

#include <batteries/math.hpp>

#include <x86gprintrin.h>

namespace turtle_kv {

using batt::bit_count;

/** \brief Returns the 0-based index of the first 1 bit in the set, or 64 if there are no 1 bits
 * present.
 */
inline i32 first_bit(u64 bit_set)
{
  if (bit_set == 0) {
    return 64;
  }
  return __builtin_ctzll(bit_set);
}

/** \brief Returns the 0-based index of the last 1 bit in the set, or -1 if there are no 1 bits
 * present.
 */
inline i32 last_bit(u64 bit_set)
{
  if (bit_set == 0) {
    return -1;
  }
  return 63 - __builtin_clzll(bit_set);
}

/** \brief Returns the index of the next 1 bit, after `index`.
 */
inline i32 next_bit(u64 bit_set, i32 index)
{
  const u64 mask = (u64{2} << index) - 1;
  return first_bit(bit_set & ~mask);
}

/** \brief Returns the one past the index of the previous 1 bit before `index`.
 */
inline i32 prev_bit(u64 bit_set, i32 index)
{
  const u64 mask = ((u64{1} << index) - 1);
  return last_bit(bit_set & mask);
}

/** \brief Reads the bit at index within bit_set, returning true iff it is set.
 */
inline bool get_bit(u64 bit_set, i32 index)
{
  return (bit_set & (u64{1} << index)) != 0;
}

/** \brief Returns a copy of `bit_set` with the specified bit (`index`) set to 0 (false) or 1 (true)
 * depending on `value`.
 */
inline [[nodiscard]] u64 set_bit(u64 bit_set, i32 index, bool value)
{
  const u64 mask = u64{1} << index;
  if (value) {
    return bit_set | mask;
  } else {
    return bit_set & ~mask;
  }
}

/** \brief Inserts a bit into `bit_set` at the given index, shifting (up) all bits at that point and
 * beyond.
 */
inline [[nodiscard]] u64 insert_bit(u64 bit_set, i32 index, bool value)
{
  const u64 lower_mask = (u64{1} << index) - 1;
  const u64 upper_mask = ~lower_mask;

  bit_set = (bit_set & lower_mask) | ((bit_set & upper_mask) << 1);
  bit_set = set_bit(bit_set, index, value);

  return bit_set;
}

/** \brief Removes the bit at the given index, shifting all those above down by one position.
 */
inline [[nodiscard]] u64 remove_bit(u64 bit_set, i32 index)
{
  const u64 lower_mask = (u64{1} << index) - 1;
  const u64 upper_mask = (~lower_mask) << 1;

  return (bit_set & lower_mask) | ((bit_set & upper_mask) >> 1);
}

template <typename IntT>
inline u64 mask_from_interval(const CInterval<IntT>& i)
{
  u64 mask = (u64{1} << i.size()) - 1;
  return mask << i.lower_bound;
}

/** \brief Returns _one less than_ the number of 1's in `bit_set` at or before position `index`.
 *
 * If pos > 63, returns -1.
 *
 * Examples:
 *   bit_rank(0b1101011100, 0) == -1
 *                       ^
 *   bit_rank(0b1101011100, 1) == -1
 *                      ^
 *   bit_rank(0b1101011100, 2) ==  0
 *                     ^
 *   bit_rank(0b1101011100, 3) ==  1
 *                    ^
 *   bit_rank(0b1101011100, 4) ==  2
 *                   ^
 *   bit_rank(0b1101011100, 5) ==  2
 *                  ^
 *   bit_rank(0b1101011100, 6) ==  3
 *                 ^
 *   bit_rank(0b1101011100, 7) ==  3
 *                ^
 *   bit_rank(0b1101011100, 8) ==  4
 *               ^
 *   bit_rank(0b1101011100, 9) ==  5
 *              ^
 */
inline i32 bit_rank(u64 bit_set, i32 index)
{
  return __builtin_popcountll(bit_set & ((u64{2} << index) - 1)) - 1;
}

/** \brief Returns the lowest bit index with the given rank.
 *
 * Example:
 *
 * bit_set =
 * 0000000000000000000000000000010101101110010010111000110001111001
 *
 * rank = 8
 * mask = (2 << 8) - 1 =
 * 0000000000000000000000000000000000000000000000000000000111111111
 *                              ┌───────────────┘│││││││││┃┃┃┃┃┃┃┃┃
 *                              │ ┌──────────────┘││││││││┃┃┃┃┃┃┃┃┃
 *                              │ │ ┌─────────────┘│││││││┃┃┃┃┃┃┃┃┃
 *                              │ │ │┌─────────────┘││││││┃┃┃┃┃┃┃┃┃
 *                              │ │ ││ ┌────────────┘│││││┃┃┃┃┃┃┃┃┃
 *                              │ │ ││ │┌────────────┘││││┃┃┃┃┃┃┃┃┃
 *                              │ │ ││ ││┌────────────┘│││┃┃┃┃┃┃┃┃┃
 *                              │ │ ││ │││  ┌──────────┘││┃┃┃┃┃┃┃┃┃
 *                              │ │ ││ │││  │  ┌────────┘│┃┃┃┃┃┃┃┃┃
 *                              │ │ ││ │││  │  │ ┌───────┘┃┃┃┃┃┃┃┃┃
 *                              │ │ ││ │││  │  │ │┏━━━━━━━┛┃┃┃┃┃┃┃┃
 *                              │ │ ││ │││  │  │ │┃┏━━━━━━━┛┃┃┃┃┃┃┃
 *                              │ │ ││ │││  │  │ │┃┃   ┏━━━━┛┃┃┃┃┃┃
 *                              │ │ ││ │││  │  │ │┃┃   ┃┏━━━━┛┃┃┃┃┃
 *                              │ │ ││ │││  │  │ │┃┃   ┃┃   ┏━┛┃┃┃┃
 *                              │ │ ││ │││  │  │ │┃┃   ┃┃   ┃┏━┛┃┃┃
 *                              │ │ ││ │││  │  │ │┃┃   ┃┃   ┃┃┏━┛┃┃
 *                              │ │ ││ │││  │  │ │┃┃   ┃┃   ┃┃┃┏━┛┃
 *                              ▽ ▽ ▽▽ ▽▽▽  ▽  ▽ ▽▼▼   ▼▼   ▼▼▼▼  ▼
 *                         (..0010101101110010010111000110001111001) (bit_set)
 * 0000000000000000000000000000000000000000000000011000110001111001  (PDEP result)
 * |◀───────────────────────────────────────────▶│
 * CLZ = 47                                      │
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~│~~~~~~~~~~~~~~~~~
 * 6666555555555544444444443333333333222222222211111111110000000000  (bit index 10's)
 * 3210987654321098765432109876543210987654321098765432109876543210  (bit index 1's)
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~│~~~~~~~~~~~~~~~~~
 *                                               │ (63 - 47) = 16 =  (select result)
 */
inline i32 bit_select(u64 bit_set, i32 rank)
{
  return 63 - __builtin_clzll(_pdep_u64((u64{1} << (rank + 1)) - 1, bit_set));
}

}  // namespace turtle_kv
