#include <turtle_kv/kv_store.hpp>

#include <batteries/assert.hpp>

#include <iostream>

int main()
{
  namespace fs = std::filesystem;

  using turtle_kv::KVStore;
  using turtle_kv::RemoveExisting;
  using turtle_kv::Status;
  using turtle_kv::StatusOr;
  using turtle_kv::TreeOptions;
  using turtle_kv::ValueView;

  // First create a database instance.
  //
  Status create_status =
      KVStore::create(fs::path{"path/to/my/data"},
                      KVStore::Config::with_default_values(),
                      RemoveExisting{true});

  BATT_CHECK_OK(create_status);

  // Now open and use the new instance.
  //
  StatusOr<std::unique_ptr<KVStore>> opened_kv_store =
      KVStore::open(fs::path{"path/to/my/data"},
                    TreeOptions::with_default_values());

  BATT_CHECK_OK(opened_kv_store);

  // Use the key-value store.
  //
  KVStore& kv = **opened_kv_store;

  BATT_CHECK_OK(kv.put("hello", ValueView::from_str("world")));

  StatusOr<ValueView> value = kv.get("hello");
  BATT_CHECK_OK(value);
  BATT_CHECK_EQ(value->as_str(), "world");

  // Query a key that doesn't exist.
  //
  value = kv.get("no such key");
  BATT_CHECK(!value.ok());
  BATT_CHECK_EQ(value.status(), batt::StatusCode::kNotFound);

  std::cout << "Test package is working!" << std::endl;

  return 0;
}