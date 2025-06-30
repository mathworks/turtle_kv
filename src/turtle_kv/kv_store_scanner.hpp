#pragma once

#include <turtle_kv/kv_store.hpp>
#include <turtle_kv/mem_table.hpp>
#include <turtle_kv/scan_metrics.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/packed_node_page.hpp>
#include <turtle_kv/tree/segmented_level_scanner.hpp>
#include <turtle_kv/tree/subtree.hpp>

#include <turtle_kv/util/art.hpp>
#include <turtle_kv/util/stack_merger.hpp>

#include <llfs/page_loader.hpp>

#include <boost/container/static_vector.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Scanner for KVStore.
 *
 * An instance of KVStoreScanner may ONLY be used on the thread on which it was created.
 */
class KVStoreScanner
{
 public:
  static constexpr usize kMaxTreeHeight = 25;
  static constexpr usize kMaxUpdateBufferLevels = 64;
  static constexpr usize kMaxHeapSize = kMaxTreeHeight * kMaxUpdateBufferLevels;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using KVSlice = Slice<const PackedKeyValue>;

  using PackedLevel = PackedNodePage::UpdateBuffer::SegmentedLevel;

  using PackedLevelScanner =
      SegmentedLevelScanner<const PackedNodePage, const PackedLevel, llfs::PageLoader>;

  struct NodeScanState;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <ART::Synchronized kSync>
  struct MemTableScanState {
    MemTable* mem_table_;
    ART::Scanner<kSync>* art_scanner_;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct TreeLevelScanState {
    KVSlice kv_slice;
    NodeScanState* node_state;
    i32 buffer_level_i;
  };

  struct ActiveMemTableTag {
  };

  struct DeltaMemTableTag {
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct ScanLevel {
    KeyView key;

    std::variant<NoneType,
                 MemTableScanState<ART::Synchronized::kTrue>,
                 MemTableScanState<ART::Synchronized::kFalse>,
                 TreeLevelScanState>
        state_impl;

    //----- --- -- -  -  -   -

    explicit ScanLevel(const KVSlice& kv_slice, NodeScanState* frame, i32 buffer_level_i) noexcept;

    explicit ScanLevel(ActiveMemTableTag,
                       MemTable& mem_table,
                       ART::Scanner<ART::Synchronized::kTrue>& art_scanner) noexcept;

    explicit ScanLevel(DeltaMemTableTag,
                       MemTable& mem_table,
                       ART::Scanner<ART::Synchronized::kFalse>& art_scanner) noexcept;

    //----- --- -- -  -  -   -

    /** \brief Returns the current item as an EditView.
     */
    EditView item() const;

    /** \brief Returns the value of the current item.
     */
    ValueView value() const;

    /** \brief Advances to the next item at this tier, returning true if a next item was found,
     * false if the tier has been fully consumed.
     */
    bool advance();
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct ScanLevelMinHeapOrder {
    bool operator()(ScanLevel* left, ScanLevel* right) const
    {
      batt::Order order = batt::compare(get_key(left->key), get_key(right->key));
      return (order == batt::Order::Less)       //
             || ((order == batt::Order::Equal)  //
                 && left < right);
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct NodeScanState {
    u64 active_levels_;
    llfs::PinnedPage pinned_page_;
    const PackedNodePage* node_;
    i32 pivot_i_;
    boost::container::static_vector<PackedLevel, kMaxUpdateBufferLevels> levels_;
    boost::container::static_vector<PackedLevelScanner, kMaxUpdateBufferLevels> level_scanners_;

    //----- --- -- -  -  -   -

    // PackedNodePage frame
    //
    template <bool kInsertHeap>
    explicit NodeScanState(KVStoreScanner& kv_scanner,
                           llfs::PinnedPage&& page,
                           const PackedNodePage& node,
                           std::integral_constant<bool, kInsertHeap>) noexcept;

    // PackedLeafPage frame
    //
    template <bool kInsertHeap>
    explicit NodeScanState(KVStoreScanner& kv_scanner,
                           llfs::PinnedPage&& page,
                           const PackedLeafPage& leaf,
                           std::integral_constant<bool, kInsertHeap>) noexcept;

    i32 get_height() const;

    KVSlice pull_next(i32 buffer_level_i);

    void deactivate(i32 buffer_level_i);
  };

  using Item = EditView;

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  explicit KVStoreScanner(KVStore& kv_store, const KeyView& min_key) noexcept;

  /** \brief Create a scanner for a checkpoint tree only (no MemTables).
   */
  explicit KVStoreScanner(llfs::PageLoader& page_loader,
                          const llfs::PageIdSlot& root,
                          i32 tree_height,
                          const KeyView& min_key) noexcept;

  ~KVStoreScanner() noexcept;

  Status start();

  const Optional<Item>& peek();

  Optional<Item> next();

  Status status() const;

  StatusOr<usize> read(const Slice<std::pair<KeyView, ValueView>>& buffer);

  //+++++++++++-+-+--+----- --- -- -  -  -   --
 private:
  Status validate_page_layout(i32 height, const llfs::PinnedPage& pinned_page);

  template <typename InsertHeap>
  Status enter_subtree(i32 subtree_height, llfs::PageIdSlot subtree_root, InsertHeap insert_heap);

  template <typename InsertHeap>
  Status enter_leaf(llfs::PinnedPage&& pinned_page, InsertHeap insert_heap);

  template <typename InsertHeap>
  Status enter_node(llfs::PinnedPage&& pinned_page, InsertHeap insert_heap);

  Status resume();

  Status set_next_item();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using DeltaMemTableScannerStorage =
      std::aligned_storage_t<sizeof(ART::Scanner<ART::Synchronized::kFalse>),
                             alignof(ART::Scanner<ART::Synchronized::kFalse>)>;

  using ActiveMemTableScannerStorage =
      std::aligned_storage_t<sizeof(ART::Scanner<ART::Synchronized::kTrue>),
                             alignof(ART::Scanner<ART::Synchronized::kTrue>)>;

  boost::intrusive_ptr<const KVStore::State> pinned_state_;
  llfs::PageLoader& page_loader_;
  llfs::PageIdSlot root_;
  i32 tree_height_;
  KeyView min_key_;
  bool needs_resume_;
  Optional<EditView> next_item_;
  Status status_;
  Optional<ART::Scanner<ART::Synchronized::kTrue>> mem_table_scanner_;
  std::array<DeltaMemTableScannerStorage, 32> static_delta_storage_;
  DeltaMemTableScannerStorage* delta_storage_;
  boost::container::static_vector<NodeScanState, kMaxTreeHeight - 1> tree_scan_path_;
  boost::container::small_vector<ScanLevel, kMaxHeapSize + 32> scan_levels_;
  StackMerger<ScanLevel, ScanLevelMinHeapOrder, kMaxHeapSize> heap_;
};

}  // namespace turtle_kv
