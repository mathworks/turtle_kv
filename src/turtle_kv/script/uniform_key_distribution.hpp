//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_UNIFORM_KEY_DISTRIBUTION_HPP

#include <turtle_kv/script/key_distribution.hpp>

#include <xxhash.h>

namespace turtle_kv {

inline constexpr std::array<u64, 64> kHashSeeds = {
    0xce3a9eb8b885d5afull, 0x33d9975b8a739ac6ull, 0xe65d0fff49425f03ull, 0x10bb3a132ec4fabcull,
    0x88d476f6e7f2c53cull, 0xcb4905c588217f44ull, 0x54eb7b8b55ac05d6ull, 0xac0de731d7f3f97cull,
    0x998963e5d908c156ull, 0x0bdf939d3b7c1cd6ull, 0x2cf7007c36b2c966ull, 0xb53c35171f25ccceull,
    0x7d6d2ad5e3ef7ae3ull, 0xe3aaa3bf1dbffd08ull, 0xa81f70b4f8dc0f80ull, 0x1f4887ce81cdf25aull,
    0x6433a69ba9e9d9b1ull, 0xf859167265201651ull, 0xe48c6589be0ff660ull, 0xadd5250ba0e7ac09ull,
    0x833f55b86dee015full, 0xae3b000feb85dceaull, 0x0110cfeb4fe23291ull, 0xf3a5d699ab2ce23cull,
    0x7c3a2b8a1c43942cull, 0x8cb3fb6783724d25ull, 0xe3619c66bf3aa139ull, 0x3fdf358be099c7d9ull,
    0x0c38ccabc94a487full, 0x43e19e80ee4fe6edull, 0x22699c9fc26f20eeull, 0xa559cbafff2cea37ull,
    0xfbed4777b17fb16dull, 0x7197788291858011ull, 0xa9325a240f0d996eull, 0x6782b2e3766f2f76ull,
    0xbc3aca45c9d9dc36ull, 0x7b687762afe92061ull, 0x7b2a7cb985790bcfull, 0xf244ed1bc2b06f7dull,
    0x29acd54ff9cb3809ull, 0xe1926523e6f67949ull, 0x98f964fbc223bb91ull, 0xaab5ee47827c5506ull,
    0x0dab726106a4c8ddull, 0xa88bb10b8e57cdd9ull, 0xbef7ede281a687afull, 0x0e2a6b9bc5b7d6e3ull,
    0x5b6f250b605200c8ull, 0xafe46bbd0e81722full, 0xb5d978e72ac594daull, 0x8c4362498b85fff9ull,
    0xce8cd0d29a933471ull, 0x9c2a28aabd1e71cbull, 0x572c8c1d4ea24d86ull, 0x8fc7dff3afb5fbf7ull,
    0xf378bc6c41606bf9ull, 0xa4c36401cf7a557full, 0x0b0a5bdd27f682afull, 0x3fbe0f66ef4777c1ull,
    0x0ed678ccbd246356ull, 0xc2d3489afc4edcd6ull, 0xc482a884240966c6ull, 0x19b952db37267518ull,
};

class UniformKeyDistribution : public KeyDistribution
{
 public:
  KeyView get_key(usize ordinal, SmallVecBase<char>& key_buffer, usize key_size) override
  {
    key_buffer.resize(key_size);

    char* p_dst = key_buffer.data();
    usize dst_n = key_size;

    for (usize j = 0; j < kHashSeeds.size(); ++j) {
      u64 hash_val = XXH64(&ordinal, sizeof(ordinal), /*seed=*/kHashSeeds[j]);
      usize n_to_copy = std::min(dst_n, sizeof(hash_val));

      std::memcpy(p_dst, &hash_val, n_to_copy);

      dst_n -= n_to_copy;
      p_dst += n_to_copy;

      if (!dst_n) {
        break;
      }
    }

    return KeyView{key_buffer.data(), key_buffer.size()};
  }
};

}  // namespace turtle_kv
