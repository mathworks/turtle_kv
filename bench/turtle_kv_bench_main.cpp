#include "turtle_kv_driver.hpp"

#include <keyvcr/player_main.hpp>

int main(int argc, char** argv)
{
  return keyvcr::player_main<turtle_kv::bench::KVStoreDriver>(argc, argv, "turtlekv");
}
