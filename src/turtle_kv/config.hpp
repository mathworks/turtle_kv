#pragma once

#define TURTLE_KV_USE_BLOOM_FILTER 0
#define TURTLE_KV_USE_QUOTIENT_FILTER 1

#if ((TURTLE_KV_USE_BLOOM_FILTER) + (TURTLE_KV_USE_QUOTIENT_FILTER)) > 1
#error You must choose one kind of filter!
#endif
