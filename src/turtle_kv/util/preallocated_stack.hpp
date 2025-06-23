#pragma once

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/continuation.hpp>

#include <boost/context/stack_context.hpp>

namespace turtle_kv {

class PreallocatedStack
{
 public:
  class Allocator
  {
   public:
    explicit Allocator() : stack_{nullptr}
    {
    }

    explicit Allocator(PreallocatedStack& stack) noexcept : stack_{&stack}
    {
    }

    boost::context::stack_context allocate() const
    {
      BATT_CHECK_NOT_NULLPTR(this->stack_);
      return this->stack_->allocate();
    }

    void deallocate(boost::context::stack_context& ctx) const
    {
      BATT_CHECK_NOT_NULLPTR(this->stack_);
      this->stack_->deallocate(ctx);
    }

   private:
    PreallocatedStack* stack_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PreallocatedStack(const MutableBuffer& buffer) noexcept : buffer_{buffer}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u8* top() const
  {
    return static_cast<u8*>(this->buffer_.data());
  }

  u8* bottom() const
  {
    return this->top() + this->buffer_.size();
  }

  usize size() const
  {
    return this->buffer_.size();
  }

  batt::StackAllocator get_allocator()
  {
    return batt::StackAllocator{Allocator{*this}};
  }

  boost::context::stack_context allocate()
  {
    BATT_CHECK(!this->in_use_);
    this->in_use_ = true;

    boost::context::stack_context ctx;
    ctx.sp = this->bottom();
    ctx.size = this->size();

    return ctx;
  }

  void deallocate(boost::context::stack_context& ctx)
  {
    BATT_CHECK(this->in_use_);
    this->in_use_ = false;

    BATT_CHECK_EQ(ctx.sp, this->bottom());
    BATT_CHECK_EQ(ctx.size, this->size());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  bool in_use_ = false;
  MutableBuffer buffer_;
};

}  // namespace turtle_kv
