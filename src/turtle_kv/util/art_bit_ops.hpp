#pragma once
#define TURTLE_KV_ART_BIT_OPS_HPP

#include <turtle_kv/import/int_types.hpp>

#include <batteries/bit_ops/first_bit.hpp>
#include <batteries/bit_ops/next_bit.hpp>

#include <emmintrin.h>  // SSE2
#include <mmintrin.h>   // MMX
#include <pmmintrin.h>  // SSE3

#ifdef __AVX512F__
#include <immintrin.h>  // AVX512 (AVX, AVX2, FMA)
#endif

namespace turtle_kv {

using batt::first_bit;
using batt::next_bit;

/** \brief Returns the index of `key_byte` in the array `keys`, if present; else returns one of: {4,
 * 5, 6, 7}.
 */
inline usize index_of(u8 key_byte, const std::array<u8, 4>& keys)
{
  __m64 pattern = _mm_set1_pi8((char)key_byte);
  u64 extended = *((const u32*)keys.data());
  __m64 values = _mm_cvtsi64_m64(extended);
  __m64 result = _m_pcmpeqb(pattern, values);

  return ((__builtin_ffsll((i64)result) - 1) >> 3) & 7;
}

/** \brief Returns the index of `key_byte` in the array `keys`, if present; else returns 31.
 */
inline usize index_of(u8 key_byte, const std::array<u8, 16>& keys)
{
  __m128i pattern = _mm_set1_epi8((char)key_byte);
  __m128i values = _mm_lddqu_si128((const __m128i*)keys.data());

#ifndef __AVX512F__
  int result = _mm_movemask_epi8(_mm_cmpeq_epi8(pattern, values));
#else
  __mmask16 result = _mm_cmpeq_epi8_mask(pattern, values);
#endif

  return (__builtin_ffs(result) - 1) & 31;
}

}  // namespace turtle_kv
