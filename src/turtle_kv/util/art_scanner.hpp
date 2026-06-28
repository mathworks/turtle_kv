#pragma once
#define TURTLE_KV_UTIL_ART_SCANNER_HPP

#include "art_base.hpp"

#include "detail/scanner_item_storage_base.hpp"
#include "detail/scanner_value_storage_base.hpp"

#include <turtle_kv/import/optional.hpp>

namespace turtle_kv {
namespace detail {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename AlignedStorageT, typename ValueT>
NodeT& scanner_view_of(usize node_prefix_len,
                       NodeT* node,
                       AlignedStorageT* storage,
                       std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kTrue>,
                       const Optional<bool>&,
                       void* value_storage_addr,
                       batt::StaticType<ValueT> type_of_value)
{
  NodeT& node_view = *(new (storage) NodeT{ARTBase::NoInit{}});

  // Retry the node read until we get a consistent view.
  //
  for (;;) {
    SeqMutex<u32>::ReadLock read_lock{node->mutex_};
    node_view.assign_from(*node, /*prefix_offset=*/node_prefix_len);
    if (node->is_terminal()) {
      ARTBase::construct_value_copy_addr(node, value_storage_addr, type_of_value);
    }
    if (!read_lock.changed()) {
      break;
    }
  }

  return node_view;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename AlignedStorageT, typename ValueT>
NodeT& scanner_view_of(usize,
                       NodeT* node,
                       AlignedStorageT*,
                       std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kFalse>,
                       const Optional<bool>& /*sync*/,
                       const void* /*value_storage_addr*/,
                       batt::StaticType<ValueT> /*type_of_value*/)
{
  return *node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename AlignedStorageT, typename ValueT>
NodeT& scanner_view_of(
    usize node_prefix_len,
    NodeT* node,
    AlignedStorageT* storage,
    std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kDynamic>,
    const Optional<bool>& sync,
    void* value_storage_addr,
    batt::StaticType<ValueT> type_of_value)
{
  if (sync.value_or(true)) {
    return scanner_view_of(
        node_prefix_len,
        node,
        storage,
        std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kTrue>{},
        sync,
        value_storage_addr,
        type_of_value);
  }
  return scanner_view_of(
      node_prefix_len,
      node,
      storage,
      std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kFalse>{},
      sync,
      value_storage_addr,
      type_of_value);
}

}  // namespace detail

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Scanner for an ART.
 *
 *  \tparam ValueT The value type stored in the scanned ART
 *  \tparam kSynchronized (true, false, dynmamic) The concurrency control for this scanner
 *  \tparam kValuesOnly When true, the scanner does not build/store key (path) information as it is
 *                      traversing items -- only values are available
 */
template <typename ValueT>
template <ARTBase::Synchronized kSynchronized, bool kValuesOnly>
class ART<ValueT>::Scanner
    : public detail::ScannerItemStorageBase<ValueT, kSynchronized, kValuesOnly>
{
 public:
  using LeafNode = ARTBase::LeafNode;
  using Node4 = ARTBase::Node4;
  using Node16 = ARTBase::Node16;
  using Node48 = ARTBase::Node48;
  using Node256 = ARTBase::Node256;

  using NodeScanState = std::variant<batt::NoneType,
                                     LeafNode::ScanState,
                                     Node4::ScanState,
                                     Node16::ScanState,
                                     Node48::ScanState,
                                     Node256::ScanState>;

  static constexpr usize kMaxDepth = ART<ValueT>::kMaxKeyLen;

  using SyncType = std::integral_constant<ARTBase::Synchronized, kSynchronized>;

  using Value = std::conditional_t<std::is_same_v<ValueT, void>,
                                   struct get_value_Not_Supported_If_ValueT_Is_Void,
                                   ValueT>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static_assert(sizeof(Node256) > sizeof(Node48));
  static_assert(sizeof(Node256) > sizeof(Node16));
  static_assert(sizeof(Node256) > sizeof(Node4));
  static_assert(sizeof(Node256) > sizeof(LeafNode));

  struct Frame {
    static constexpr usize kStorageSize =
        ((kSynchronized == ARTBase::Synchronized::kFalse) ? 1 : sizeof(Node256));

    std::aligned_storage_t<kStorageSize, alignof(usize)> node_storage_;
    NodeScanState scan_state_;
    usize key_prefix_len_;
    std::string_view lower_bound_key_;
    ByteInt min_key_byte_;

    explicit Frame(usize key_prefix_len, std::string_view lower_bound_key) noexcept
        : scan_state_{None}
        , key_prefix_len_{key_prefix_len}
        , lower_bound_key_{lower_bound_key}
        , min_key_byte_{ByteInt::from_i32(0)}
    {
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::aligned_storage_t<sizeof(Frame) * kMaxDepth, /*alignment=*/64> stack_storage_;
  Frame* end_ = reinterpret_cast<Frame*>(&this->stack_storage_);
  usize depth_ = 0;
  bool have_item_ = false;
  ValueT* next_value_ = nullptr;
  Optional<bool> synchronized_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** Resets the "have item" state of the scanner; after calling, have item will be false.
   */
  void reset_item() BATT_ALWAYS_INLINE
  {
    this->have_item_ = false;
    if (!std::is_same_v<ValueT, void>) {
      this->next_value_ = nullptr;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 public:
  explicit Scanner(ART& art,
                   std::string_view lower_bound_key,
                   Optional<bool> synchronized = None) noexcept
      : synchronized_{synchronized}
  {
    NodeBase* root = nullptr;
    for (;;) {
      SeqMutex<u32>::ReadLock root_read_lock{art.super_root_.mutex_};
      root = art.root_;
      if (!root_read_lock.changed()) {
        break;
      }
    }

    if (root) {
      root->visit([&](auto* node) {
        this->enter(node, /*key_prefix_len=*/0, lower_bound_key);
      });

      if (!this->have_item_) {
        this->advance();
      }
    }
  }

  ~Scanner() noexcept
  {
  }

  bool is_synchronized() const
  {
    if (kSynchronized == ARTBase::Synchronized::kFalse) {
      return false;
    }
    if (kSynchronized == ARTBase::Synchronized::kTrue) {
      return true;
    }
    return this->synchronized_.value_or(true);
  }

  template <typename NodeT>
  void enter(NodeT* node, usize key_prefix_len, std::string_view lower_bound_key)
  {
    Frame* top = new (this->end_) Frame{key_prefix_len, lower_bound_key};
    ++this->depth_;
    ++this->end_;

    // Node prefix is immutable, so we don't need synchronization.
    //
    const char* const node_prefix = node->prefix();
    const usize node_prefix_len = node->prefix_len_;

    // We need to create a copy of the node data to protect against data races.
    //
    NodeT& node_view =
        detail::scanner_view_of(node_prefix_len,
                                node,
                                &top->node_storage_,
                                SyncType{},
                                this->synchronized_,
                                this->value_storage_address(node, this->synchronized_),
                                batt::StaticType<ValueT>{});

    // Compare the lower bound key to the current node prefix.
    //
    const usize compare_len = std::min<usize>(node_prefix_len, top->lower_bound_key_.size());
    if (compare_len) {
      const i32 order = __builtin_memcmp(node_prefix, top->lower_bound_key_.data(), compare_len);

      // If all keys in this subtree come before the lower bound, then there is nothing to do.
      //
      if (order < 0) {
        --this->depth_;
        --this->end_;
        return;
      }

      // If the node prefix is a prefix of the lower bound key, then drop the prefix from the lower
      // bound; otherwise the node prefix comes *after* the lower bound, so we can safely ignore the
      // lower bound for the rest of the recursion.
      //
      if (order == 0 && compare_len == node_prefix_len) {
        top->lower_bound_key_.remove_prefix(compare_len);
      } else {
        top->lower_bound_key_ = {};
      }
    }

    // Set bounds for branch visitation.
    //
    top->min_key_byte_ = [&]() -> ByteInt {
      if (top->lower_bound_key_.empty()) {
        return ByteInt::from_i32(0);
      }
      const ByteInt next_char = ByteInt::from_char(top->lower_bound_key_.front());
      top->lower_bound_key_.remove_prefix(1);
      return next_char;
    }();

    // Append the node prefix to the buffer.
    //
    if (node_prefix_len) {
      this->append_key(top->key_prefix_len_, node_prefix, node_prefix_len);
      top->key_prefix_len_ += node_prefix_len;
    }

    // If the current node is a key-terminal, emit the contents of the buffer.
    //
    if (node_view.is_terminal()) {
      this->have_item_ = true;
      this->set_key_len(top->key_prefix_len_);
      if (!std::is_same_v<ValueT, void>) {
        this->next_value_ = (ValueT*)(this->value_storage_address(&node_view, this->synchronized_));
      }
    } else {
      this->reset_item();
    }

    [[maybe_unused]] auto& scan_state_impl =
        top->scan_state_.template emplace<typename NodeT::ScanState>(node_view, top->min_key_byte_);
  }

  bool is_done() const
  {
    return this->depth_ == 0;
  }

  // get_key() const member function is inherited from ItemStorageBase, if kValuesOnly is false.

  const Value& get_value() const
  {
    static_assert(!std::is_same_v<ValueT, void>);
    return *this->next_value_;
  }

  void advance()
  {
    this->reset_item();

    for (;;) {
      if (this->depth_ == 0) {
        return;
      }

      Frame* top = this->end_ - 1;

      batt::case_of(
          top->scan_state_,
          [](batt::NoneType&) {
            BATT_PANIC() << "empty Scanner stack frame!";
          },
          [&](auto& scan_state)
              -> std::enable_if_t<
                  !std::is_same_v<std::decay_t<decltype(scan_state)>, batt::NoneType>> {
            //----- --- -- -  -  -   -
            if (scan_state.is_done()) {
              --this->depth_;
              --this->end_;
              return;
            }

            const ByteInt key_byte = scan_state.get_key_byte();
            NodeBase* const child = scan_state.get_branch();

            this->append_key_byte(top->key_prefix_len_, key_byte);

            if (key_byte == top->min_key_byte_) {
              child->visit([&](auto* child_node) {
                this->enter(child_node, top->key_prefix_len_ + 1, top->lower_bound_key_);
              });
            } else {
              child->visit([&](auto* child_node) {
                this->enter(child_node, top->key_prefix_len_ + 1, std::string_view{});
              });
            }

            scan_state.advance();
          });

      if (this->have_item_) {
        return;
      }
    }
  }
};

}  // namespace turtle_kv
