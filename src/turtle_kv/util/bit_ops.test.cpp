#include <turtle_kv/util/bit_ops.hpp>
//
#include <turtle_kv/util/bit_ops.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/import/optional.hpp>

#include <batteries/stream_util.hpp>

#include <random>

namespace {

using namespace turtle_kv::int_types;

using turtle_kv::bit_count;
using turtle_kv::bit_rank;
using turtle_kv::bit_select;
using turtle_kv::first_bit;
using turtle_kv::get_bit;
using turtle_kv::last_bit;
using turtle_kv::next_bit;
using turtle_kv::Optional;
using turtle_kv::prev_bit;
using turtle_kv::set_bit;

/** \brief A slow but trivially correct implementation of bit_rank.
 */
i32 slow_bit_rank(u64 bit_set, i32 index)
{
    index = std::min<i32>(index, 63);
    i32 rank = -1;
    for (i32 j = 0; j <= index; ++j) {
        if (get_bit(bit_set, j)) {
            ++rank;
        }
    }
    return rank;
}

/** \brief A slow but trivially correct implementation of bit_select.
 */
i32 slow_bit_select(u64 bit_set, i32 rank)
{
    for (i32 index = 0; index < 64; ++index) {
        if (slow_bit_rank(bit_set, index) == rank) {
            return index;
        }
    }
    return 64;
}

/** \brief A slow but trivially correct implementation of first_bit.
 */
i32 slow_first_bit(u64 bit_set)
{
    for (i32 index = 0; index < 64; ++index) {
        if (get_bit(bit_set, index)) {
            return index;
        }
    }
    return 64;
}

/** \brief A slow but trivially correct implementation of next_bit.
 */
i32 slow_next_bit(u64 bit_set, i32 index)
{
    for (++index; index < 64; ++index) {
        if (get_bit(bit_set, index)) {
            return index;
        }
    }
    return 64;
}

/** \brief A slow but trivially correct implementation of last_bit.
 */
i32 slow_last_bit(u64 bit_set)
{
    for (i32 index = 64; index > 0;) {
        --index;
        if (get_bit(bit_set, index)) {
            return index;
        }
    }
    return -1;
}

/** \brief A slow but trivially correct implementation of last_bit.
 */
i32 slow_prev_bit(u64 bit_set, i32 index)
{
    for (--index; index >= 0; --index) {
        if (get_bit(bit_set, index)) {
            return index;
        }
    }
    return -1;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IndexBenchTest, BitRankSelect)
{
    for (i32 pos = 0; pos <= 64; ++pos) {
        EXPECT_EQ(bit_rank(u64{0}, pos), -1);
    }

    EXPECT_EQ(bit_rank(0b1101011100, 0), -1);
    EXPECT_EQ(bit_rank(0b1101011100, 1), -1);
    EXPECT_EQ(bit_rank(0b1101011100, 2), 0);
    EXPECT_EQ(bit_rank(0b1101011100, 3), 1);
    EXPECT_EQ(bit_rank(0b1101011100, 4), 2);
    EXPECT_EQ(bit_rank(0b1101011100, 5), 2);
    EXPECT_EQ(bit_rank(0b1101011100, 6), 3);
    EXPECT_EQ(bit_rank(0b1101011100, 7), 3);
    EXPECT_EQ(bit_rank(0b1101011100, 8), 4);
    EXPECT_EQ(bit_rank(0b1101011100, 9), 5);

    for (i32 pos = 10; pos < 64; ++pos) {
        EXPECT_EQ(bit_rank(0b1101011100, pos), 5) << BATT_INSPECT(pos);
    }
    EXPECT_EQ(bit_rank(0b1101011100, 64), 5);

    std::default_random_engine rng{std::random_device{}()};
    std::uniform_int_distribution<u64> pick_u64{0, ~u64{0}};

    for (usize i = 0; i < 100000; ++i) {
        const u64 b = pick_u64(rng);

        ASSERT_EQ(bit_rank(b, 0), get_bit(b, 0) ? 0 : -1);
        ASSERT_EQ(bit_rank(b, 63), bit_count(b) - 1);
        ASSERT_EQ(bit_rank(b, 64), bit_count(b) - 1);

        for (i32 rank = 0; rank < bit_count(b); ++rank) {
            ASSERT_EQ(slow_bit_select(b, rank), bit_select(b, rank)) << BATT_INSPECT(rank) << std::endl
                                                                     << std::bitset<64>{b};

            i32 index = bit_select(b, rank);
            ASSERT_EQ(bit_rank(b, index), rank) << BATT_INSPECT(index);
        }

        for (i32 index = 0; index < 64; ++index) {
            ASSERT_EQ(slow_bit_rank(b, index), bit_rank(b, index)) << BATT_INSPECT(index) << std::endl
                                                                   << std::bitset<64>{b};

            i32 rank_at_index = bit_rank(b, index);
            if (get_bit(b, index)) {
                ASSERT_EQ(bit_select(b, rank_at_index), index) << BATT_INSPECT(rank_at_index) << std::endl
                                                               << std::bitset<64>{b};
            } else {
                ASSERT_LT(bit_select(b, rank_at_index), index) << BATT_INSPECT(rank_at_index) << std::endl
                                                               << std::bitset<64>{b};
            }
        }
    }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BitOpsTest, NextPrev)
{
    for (i32 index = 0; index < 64; ++index) {
        ASSERT_EQ(next_bit(u64{0}, index), 64);
    }

    {
        for (usize i = 0; i < 64; ++i) {
            for (usize j = 0; j < i; ++j) {
                u64 b = 0;
                b = set_bit(b, i, true);
                b = set_bit(b, j, true);

                ASSERT_EQ(next_bit(b, j), i)
                    << BATT_INSPECT(std::bitset<64>{b}) << BATT_INSPECT(i) << BATT_INSPECT(j);

                if (i != 0) {
                    ASSERT_EQ(prev_bit(b, i), j)
                        << BATT_INSPECT(std::bitset<64>{b}) << BATT_INSPECT(i) << BATT_INSPECT(j)
                        << BATT_INSPECT(batt::to_string(std::hex, prev_bit(b, i)));
                }

                if (j != 0) {
                    ASSERT_EQ(prev_bit(b, j), -1)
                        << BATT_INSPECT(std::bitset<64>{b}) << BATT_INSPECT(i) << BATT_INSPECT(j)
                        << BATT_INSPECT(batt::to_string(std::hex, prev_bit(b, j)));
                }

                ASSERT_EQ(next_bit(b, i), 64)
                    << BATT_INSPECT(std::bitset<64>{b}) << BATT_INSPECT(i) << BATT_INSPECT(j);
            }
        }
    }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BitOpsTest, FirstNextCount)
{
    std::default_random_engine rng{std::random_device{}()};
    std::uniform_int_distribution<u64> pick_u64{0, ~u64{0}};

    for (usize case_i = 0; case_i < 100000; ++case_i) {
        const u64 bit_set = pick_u64(rng);

        // create a copy using first/next
        //
        u64 bit_set_copy = 0;
        usize count = 0;

        Optional<usize> prev_i;
        usize bit_i = first_bit(bit_set);
        EXPECT_EQ(bit_i, slow_first_bit(bit_set));

        while (bit_i < 64) {
            if (prev_i) {
                ASSERT_GT(bit_i, *prev_i)
                    << BATT_INSPECT(std::bitset<64>{bit_set}) << BATT_INSPECT(count) << BATT_INSPECT(case_i);
            }
            prev_i = bit_i;
            ++count;
            bit_set_copy = set_bit(bit_set_copy, bit_i, true);
            EXPECT_EQ(next_bit(bit_set, bit_i), slow_next_bit(bit_set, bit_i));
            bit_i = next_bit(bit_set, bit_i);
        }

        ASSERT_EQ(bit_set_copy, bit_set);
        ASSERT_EQ(count, bit_count(bit_set));
    }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BitOpsTest, LastPrevCount)
{
    std::default_random_engine rng{std::random_device{}()};
    std::uniform_int_distribution<u64> pick_u64{0, ~u64{0}};

    for (usize case_i = 0; case_i < 100000; ++case_i) {
        const u64 bit_set = pick_u64(rng);

        // create a copy using first/next
        //
        u64 bit_set_copy = 0;
        usize count = 0;

        Optional<i32> prev_i;
        i32 bit_i = last_bit(bit_set);
        EXPECT_EQ(bit_i, slow_last_bit(bit_set)) << BATT_INSPECT(std::bitset<64>{bit_set});

        while (bit_i >= 0) {
            if (prev_i) {
                ASSERT_LT(bit_i, *prev_i)
                    << BATT_INSPECT(std::bitset<64>{bit_set}) << BATT_INSPECT(count) << BATT_INSPECT(case_i);
            }
            prev_i = bit_i;
            ++count;
            bit_set_copy = set_bit(bit_set_copy, bit_i, true);

            EXPECT_EQ(prev_bit(bit_set, bit_i), slow_prev_bit(bit_set, bit_i))
                << BATT_INSPECT(bit_i) << BATT_INSPECT(std::bitset<64>{bit_set});

            bit_i = prev_bit(bit_set, bit_i);
        }
        EXPECT_EQ(count, bit_count(bit_set));
        ASSERT_EQ(bit_set_copy, bit_set) << std::endl
                                         << std::bitset<64>{bit_set_copy} << std::endl
                                         << std::bitset<64>{bit_set};
    }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(BitOpsTest, InsertRemove)
{
    using turtle_kv::get_bit;
    using turtle_kv::insert_bit;
    using turtle_kv::remove_bit;

    // (To make the formatting line up below)
    //
    constexpr bool true_ = true;

    const u64 b{0b10100100011100100011001010000110};
    //            33222222222211111111110000000000
    //            10987654321098765432109876543210
    //       a:  |||             |              |^
    //       b:  |||             |              ^
    //       c:  |||             ^
    //       d:  ||^
    //       e:  |^
    //       f:  ^

    // Test: insert_bit is the inverse of remove_bit.
    //
    for (i32 index = 0; index < 64; ++index) {
        EXPECT_EQ(insert_bit(remove_bit(b, index), index, get_bit(b, index)), b);
        EXPECT_EQ(remove_bit(insert_bit(b, index, false), index), b);
        EXPECT_EQ(remove_bit(insert_bit(b, index, true_), index), b);
    }

#define EXPECT_BITS_EQ(a, b) EXPECT_EQ(a, b) << "\n" << std::bitset<64>{a} << "\n" << std::bitset<64>{b}

    // a: (index=0)
    //
    EXPECT_BITS_EQ(insert_bit(b, 0, false), u64{0b101001000111001000110010100001100});
    EXPECT_BITS_EQ(insert_bit(b, 0, true_), u64{0b101001000111001000110010100001101});
    //                                             33222222222211111111110000000000
    //                                             10987654321098765432109876543210
    //                                        a:                                  ^

    // b: (index=1)
    //
    EXPECT_BITS_EQ(insert_bit(b, 1, false), u64{0b101001000111001000110010100001100});
    EXPECT_BITS_EQ(insert_bit(b, 1, true_), u64{0b101001000111001000110010100001110});
    //                                             33222222222211111111110000000000
    //                                             10987654321098765432109876543210
    //                                        b:                                 ^

    // c: (index=16)
    //
    EXPECT_BITS_EQ(insert_bit(b, 16, false), u64{0b101001000111001000011001010000110});
    EXPECT_BITS_EQ(insert_bit(b, 16, true_), u64{0b101001000111001010011001010000110});
    //                                              33222222222211111111110000000000
    //                                              10987654321098765432109876543210
    //                                         c:                  ^

    // d: (index=30)
    //
    EXPECT_BITS_EQ(insert_bit(b, 30, false), u64{0b100100100011100100011001010000110});
    EXPECT_BITS_EQ(insert_bit(b, 30, true_), u64{0b101100100011100100011001010000110});
    //                                              33222222222211111111110000000000
    //                                              10987654321098765432109876543210
    //                                         d:    ^

    // e: (index=31)
    //
    EXPECT_BITS_EQ(insert_bit(b, 31, false), u64{0b100100100011100100011001010000110});
    EXPECT_BITS_EQ(insert_bit(b, 31, true_), u64{0b110100100011100100011001010000110});
    //                                              33222222222211111111110000000000
    //                                              10987654321098765432109876543210
    //                                         e:   ^

    // f: (index=32)
    //
    EXPECT_BITS_EQ(insert_bit(b, 32, false), u64{0b010100100011100100011001010000110});
    EXPECT_BITS_EQ(insert_bit(b, 32, true_), u64{0b110100100011100100011001010000110});
    //                                              33222222222211111111110000000000
    //                                              10987654321098765432109876543210
    //                                         f:  ^
}

}  // namespace
