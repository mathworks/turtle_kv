#pragma once

#include <variant>

namespace turtle_kv {

struct InMemoryNodeEmptyLevel;
struct InMemoryNodeMergedLevel;
struct InMemoryNodeSegmentedLevel;
struct InMemoryNodeHybridLevel;

using InMemoryNodeLevel = std::variant<InMemoryNodeEmptyLevel,
                                       InMemoryNodeMergedLevel,
                                       InMemoryNodeSegmentedLevel,
                                       InMemoryNodeHybridLevel>;

}  // namespace turtle_kv
