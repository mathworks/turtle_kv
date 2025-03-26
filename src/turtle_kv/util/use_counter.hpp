#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <batteries/async/watch.hpp>

#include <boost/intrusive_ptr.hpp>

namespace turtle_kv {

class UseCounter
{
 public:
  static boost::intrusive_ptr<UseCounter> make_new()
  {
    return boost::intrusive_ptr<UseCounter>{new UseCounter{}};
  }

  UseCounter(const UseCounter&) = delete;
  UseCounter& operator=(const UseCounter&) = delete;

  const batt::Watch<i64>& counter() const
  {
    return this->counter_;
  }

  void halt()
  {
    this->counter_.close();
  }

  friend void intrusive_ptr_add_ref(UseCounter*);
  friend void intrusive_ptr_release(UseCounter*);

 private:
  UseCounter() = default;

  batt::Watch<i64> counter_{0};
};

inline void intrusive_ptr_add_ref(UseCounter* this_)
{
  this_->counter_.fetch_add(1);
}

inline void intrusive_ptr_release(UseCounter* this_)
{
  if (this_->counter_.fetch_sub(1) == 1) {
    delete this_;
  }
}

}  // namespace turtle_kv
