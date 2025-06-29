#pragma once

#include <turtle_kv/scan_metrics.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/packed_node_page.hpp>
#include <turtle_kv/tree/segmented_level_scanner.hpp>
#include <turtle_kv/tree/subtree.hpp>

#include <turtle_kv/util/stack_merger.hpp>

#include <llfs/page_loader.hpp>

#include <boost/container/static_vector.hpp>

namespace turtle_kv {

#if 1

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// NEW IMPL
//
class CheckpointTreeScanner
{
 public:
  static constexpr usize kMaxTreeHeight = 25;
  static constexpr usize kMaxUpdateBufferLevels = 64;
  static constexpr usize kMaxHeapSize = kMaxTreeHeight * kMaxUpdateBufferLevels;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using KVSlice = Slice<const PackedKeyValue>;

  struct KVSliceMinHeapOrder {
    bool operator()(KVSlice* left, KVSlice* right) const
    {
      batt::Order order = batt::compare(get_key(left->front()), get_key(right->front()));
      return (order == batt::Order::Less)       //
             || ((order == batt::Order::Equal)  //
                 && left < right);
    }
  };

  using Item = EditView;

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  explicit CheckpointTreeScanner(llfs::PageLoader& page_loader,
                                 const llfs::PageIdSlot& root,
                                 i32 tree_height,
                                 const KeyView& min_key) noexcept
      : page_loader_{page_loader}
      , root_{root}
      , tree_height_{tree_height}
      , min_key_{min_key}
      , heap_{}
  {
  }

  Status start()
  {
    // Handle the empty tree case.
    //
    if (!this->root_.is_valid()) {
      return OkStatus();
    }

    BATT_REQUIRE_OK(this->enter_subtree(this->tree_height_, this->root_, std::false_type{}));

    // Run make heap once at the beginning.
    //
    this->heap_.reset(as_slice(this->tiers_), /*minimum_capacity=*/kMaxHeapSize);

    BATT_REQUIRE_OK(this->set_next_item());

    return OkStatus();
  }

  const Optional<Item>& peek()
  {
    return this->next_item_;
  }

  Optional<Item> next()
  {
    Optional<Item> item;
    std::swap(item, this->next_item_);
    if (item) {
      this->status_.Update(this->set_next_item());
    }
    return item;
  }

  Status status() const
  {
    return this->status_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --
 private:
  using PackedLevel = PackedNodePage::UpdateBuffer::SegmentedLevel;

  using PackedLevelScanner =
      SegmentedLevelScanner<const PackedNodePage, const PackedLevel, llfs::PageLoader>;

  struct PathFrame;

  struct PathFrameLocation {
    PathFrame* frame;
    i32 level_i;
  };

  struct PathFrame {
    u64 active_levels_ = 0;
    llfs::PinnedPage pinned_page_;
    const PackedNodePage* node_;
    i32 pivot_i_;
    boost::container::static_vector<PackedLevel, kMaxUpdateBufferLevels> levels_;
    boost::container::static_vector<PackedLevelScanner, kMaxUpdateBufferLevels> level_scanners_;

    //----- --- -- -  -  -   -

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // PackedNodePage frame
    //
    template <bool kInsertHeap>
    explicit PathFrame(CheckpointTreeScanner& tree_scanner,
                       llfs::PinnedPage&& page,
                       const PackedNodePage& node,
                       std::integral_constant<bool, kInsertHeap>) noexcept
        : pinned_page_{std::move(page)}
        , node_{&node}
        , pivot_i_{(i32)in_node(node).find_pivot_containing(tree_scanner.min_key_)}
    {
      const i32 n_levels = this->node_->get_level_count();

      for (i32 level_i = 0; level_i < n_levels; ++level_i) {
        PackedLevel& level = this->levels_.emplace_back(this->node_->is_size_tiered()
                                                            ? this->node_->get_tier(level_i)
                                                            : this->node_->get_level(level_i));

        this->level_scanners_.emplace_back(*this->node_,
                                           level,
                                           tree_scanner.page_loader_,
                                           llfs::PinPageToJob::kFalse,
                                           this->pivot_i_,
                                           tree_scanner.min_key_);

        KVSlice first_slice = this->pull_next(level_i);

        if (!first_slice.empty()) {
          this->active_levels_ |= (u64{1} << level_i);
          tree_scanner.frame_for_tier_.emplace_back(PathFrameLocation{
              .frame = this,
              .level_i = level_i,
          });
          KVSlice& tier = tree_scanner.tiers_.emplace_back(first_slice);
          if (kInsertHeap) {
            tree_scanner.heap_.insert(&tier);
          }
        }
      }
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // PackedLeafPage frame
    //
    template <bool kInsertHeap>
    explicit PathFrame(CheckpointTreeScanner& tree_scanner,
                       llfs::PinnedPage&& page,
                       const PackedLeafPage& leaf,
                       std::integral_constant<bool, kInsertHeap>) noexcept
        : pinned_page_{std::move(page)}
        , node_{nullptr}
        , pivot_i_{0}
    {
      KVSlice first_slice = as_slice(leaf.lower_bound(tree_scanner.min_key_), leaf.items_end());
      if (first_slice.empty()) {
        return;
      }

      this->active_levels_ = 1;
      tree_scanner.frame_for_tier_.emplace_back(PathFrameLocation{
          .frame = this,
          .level_i = 0,
      });
      KVSlice& tier = tree_scanner.tiers_.emplace_back(first_slice);
      if (kInsertHeap) {
        tree_scanner.heap_.insert(&tier);
      }
    }

    i32 get_height() const
    {
      if (!this->node_) {
        return 1;
      }
      return this->node_->height;
    }

    KVSlice pull_next(i32 level_i)
    {
      if (!this->node_) {
        this->deactivate(level_i);
        return KVSlice{};
      }

      PackedLevelScanner& level_scanner = this->level_scanners_[level_i];
      KVSlice* result = nullptr;
      for (;;) {
        Optional<EditSlice> slice = level_scanner.next();
        if (!slice) {
          this->deactivate(level_i);
          return KVSlice{};
        }

        batt::case_of(
            *slice,
            [](Slice<const EditView>&) {
              BATT_PANIC() << "Invalid EditSlice type: EditView";
              BATT_UNREACHABLE();
            },
            [&](Slice<const PackedKeyValue>& kv_slice) {
              if (!kv_slice.empty()) {
                result = &kv_slice;
              }
            });

        if (result) {
          return *result;
        }
      }
    }

    void deactivate(i32 level_i)
    {
      this->active_levels_ &= ~(u64{1} << level_i);
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status validate_page_layout(i32 height, const llfs::PinnedPage& pinned_page)
  {
    const auto& page_header =
        *static_cast<const llfs::PackedPageHeader*>(pinned_page->const_buffer().data());

    if (height > 1) {
      if (page_header.layout_id != NodePageView::page_layout_id()) {
        return {batt::StatusCode::kDataLoss};
      }
    } else {
      BATT_CHECK_EQ(height, 1);
      if (page_header.layout_id != LeafPageView::page_layout_id()) {
        return {batt::StatusCode::kDataLoss};
      }
    }

    return OkStatus();
  }

  template <typename InsertHeap>
  Status enter_subtree(i32 subtree_height, llfs::PageIdSlot subtree_root, InsertHeap insert_heap)
  {
    for (;;) {
      StatusOr<llfs::PinnedPage> pinned_page =
          subtree_root.load_through(this->page_loader_,
                                    llfs::PageLoadOptions{
                                        llfs::PinPageToJob::kFalse,
                                        llfs::OkIfNotFound{false},
                                        llfs::LruPriority{kNodeLruPriority},
                                    });

      BATT_REQUIRE_OK(pinned_page);
      BATT_REQUIRE_OK(this->validate_page_layout(subtree_height, *pinned_page));

      if (subtree_height == 1) {
        BATT_REQUIRE_OK(this->enter_leaf(std::move(*pinned_page), insert_heap));
        break;
      }

      BATT_REQUIRE_OK(this->enter_node(std::move(*pinned_page), insert_heap));

      PathFrame& frame = this->path_frames_.back();

      subtree_root =
          llfs::PageIdSlot::from_page_id(frame.node_->get_child_id(frame.pivot_i_).unpack());

      --subtree_height;
    }

    return OkStatus();
  }

  template <typename InsertHeap>
  Status enter_leaf(llfs::PinnedPage&& pinned_page, InsertHeap insert_heap)
  {
    const PackedLeafPage& leaf = PackedLeafPage::view_of(pinned_page);
    this->path_frames_.emplace_back(*this, std::move(pinned_page), leaf, insert_heap);
    return OkStatus();
  }

  template <typename InsertHeap>
  Status enter_node(llfs::PinnedPage&& pinned_page, InsertHeap insert_heap)
  {
    const PackedNodePage& node = PackedNodePage::view_of(pinned_page);
    this->path_frames_.emplace_back(*this, std::move(pinned_page), node, insert_heap);
    return OkStatus();
  }

  Status resume()
  {
    this->needs_resume_ = false;

    for (;;) {
      if (this->path_frames_.empty()) {
        break;
      }

      PathFrame& frame = this->path_frames_.back();

      if (frame.node_) {
        if (frame.pivot_i_ < frame.node_->pivot_count()) {
          ++frame.pivot_i_;
          if (frame.pivot_i_ != frame.node_->pivot_count()) {
            BATT_REQUIRE_OK(this->enter_subtree(
                frame.get_height() - 1,
                llfs::PageIdSlot::from_page_id(frame.node_->get_child_id(frame.pivot_i_).unpack()),
                std::true_type{}));
            continue;
          }
        }
      }

      if (frame.active_levels_ != 0) {
        break;
      }

      this->path_frames_.pop_back();
    }

    return OkStatus();
  }

  Status set_next_item()
  {
    for (;;) {
      if (this->heap_.empty()) {
        return OkStatus();
      }

      KVSlice* slice = this->heap_.first();
      BATT_CHECK(!slice->empty());

      if (!this->next_item_) {
        this->next_item_.emplace(to_edit_view(slice->front()));

      } else if (get_key(*this->next_item_) == get_key(slice->front())) {
        if (this->next_item_->needs_combine()) {
          *this->next_item_ = combine(*this->next_item_, to_edit_view(slice->front()));
        }
      } else {
        break;
      }

      slice->drop_front();

      if (slice->empty()) {
        this->heap_.remove_first();
      } else {
        this->heap_.update_first();
      }
    }

    if (this->needs_resume_) {
      BATT_REQUIRE_OK(this->resume());
    }

    return OkStatus();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageLoader& page_loader_;
  llfs::PageIdSlot root_;
  i32 tree_height_;
  KeyView min_key_;
  bool needs_resume_ = false;
  Optional<EditView> next_item_;
  Status status_;
  boost::container::static_vector<PathFrame, kMaxTreeHeight - 1> path_frames_;
  boost::container::static_vector<PathFrameLocation, kMaxHeapSize> frame_for_tier_;
  boost::container::static_vector<KVSlice, kMaxHeapSize> tiers_;
  StackMerger<KVSlice, KVSliceMinHeapOrder, kMaxHeapSize> heap_;
};

#else
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// OLD IMPL
//
class CheckpointTreeScanner
{
 public:
  static constexpr usize kMaxTreeHeight = 25;
  static constexpr usize kMaxUpdateBufferLevels = 64;
  static constexpr usize kMaxHeapSize = kMaxTreeHeight * kMaxUpdateBufferLevels;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using KVSlice = Slice<const PackedKeyValue>;

  struct KVSliceMaxHeapOrder {
    bool operator()(KVSlice* left, KVSlice* right) const
    {
      batt::Order order = batt::compare(get_key(left->front()), get_key(right->front()));
      return (order == batt::Order::Greater)    //
             || ((order == batt::Order::Equal)  //
                 && left > right);
    }
  };

  using Item = EditView;

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  explicit CheckpointTreeScanner(llfs::PageLoader& page_loader,
                                 const llfs::PageIdSlot& root,
                                 i32 tree_height,
                                 const KeyView& min_key) noexcept
      : page_loader_{page_loader}
      , root_{root}
      , tree_height_{tree_height}
      , min_key_{min_key}
  {
  }

  Status start()
  {
    // Handle the empty tree case.
    //
    if (!this->root_.is_valid()) {
      return OkStatus();
    }

    BATT_REQUIRE_OK(this->enter_subtree(this->tree_height_, this->root_));
    BATT_REQUIRE_OK(this->set_next_item());

    return OkStatus();
  }

  const Optional<Item>& peek()
  {
    return this->next_item_;
  }

  Optional<Item> next()
  {
    Optional<Item> item;
    std::swap(item, this->next_item_);
    if (item) {
      this->status_.Update(this->set_next_item());
    }
    return item;
  }

  Status status() const
  {
    return this->status_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --
 private:
  using PackedLevel = PackedNodePage::UpdateBuffer::SegmentedLevel;

  using PackedLevelScanner =
      SegmentedLevelScanner<const PackedNodePage, const PackedLevel, llfs::PageLoader>;

  struct PathFrame;

  struct PathFrameLocation {
    PathFrame* frame;
    i32 level_i;
  };

  struct PathFrame {
    u64 active_levels_ = 0;
    llfs::PinnedPage pinned_page_;
    const PackedNodePage* node_;
    i32 pivot_i_;
    boost::container::static_vector<PackedLevel, kMaxUpdateBufferLevels> levels_;
    boost::container::static_vector<PackedLevelScanner, kMaxUpdateBufferLevels> level_scanners_;

    //----- --- -- -  -  -   -

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // PackedNodePage frame
    //
    explicit PathFrame(CheckpointTreeScanner& tree_scanner,
                       llfs::PinnedPage&& page,
                       const PackedNodePage& node,
                       const KeyView& min_key) noexcept
        : pinned_page_{std::move(page)}
        , node_{&node}
        , pivot_i_{(i32)in_node(node).find_pivot_containing(min_key)}
    {
      const i32 n_levels = this->node_->get_level_count();

      for (i32 level_i = 0; level_i < n_levels; ++level_i) {
        PackedLevel& level = this->levels_.emplace_back(this->node_->is_size_tiered()
                                                            ? this->node_->get_tier(level_i)
                                                            : this->node_->get_level(level_i));

        this->level_scanners_.emplace_back(*this->node_,
                                           level,
                                           tree_scanner.page_loader_,
                                           llfs::PinPageToJob::kFalse,
                                           this->pivot_i_,
                                           min_key);

        KVSlice first_slice = this->pull_next(level_i);

        if (!first_slice.empty()) {
          this->active_levels_ |= (u64{1} << level_i);
          tree_scanner.frame_for_tier_.emplace_back(PathFrameLocation{
              .frame = this,
              .level_i = level_i,
          });
          KVSlice& tier = tree_scanner.tiers_.emplace_back(first_slice);
          tree_scanner.push_heap(&tier);
        }
      }
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // PackedLeafPage frame
    //
    explicit PathFrame(CheckpointTreeScanner& tree_scanner,
                       llfs::PinnedPage&& page,
                       const PackedLeafPage& leaf,
                       const KeyView& min_key) noexcept
        : pinned_page_{std::move(page)}
        , node_{nullptr}
        , pivot_i_{0}
    {
      KVSlice first_slice = as_slice(leaf.lower_bound(min_key), leaf.items_end());
      if (first_slice.empty()) {
        return;
      }

      this->active_levels_ = 1;
      tree_scanner.frame_for_tier_.emplace_back(PathFrameLocation{
          .frame = this,
          .level_i = 0,
      });
      KVSlice& tier = tree_scanner.tiers_.emplace_back(first_slice);
      tree_scanner.push_heap(&tier);
    }

    i32 get_height() const
    {
      if (!this->node_) {
        return 1;
      }
      return this->node_->height;
    }

    KVSlice pull_next(i32 level_i)
    {
      if (!this->node_) {
        this->deactivate(level_i);
        return KVSlice{};
      }

      PackedLevelScanner& level_scanner = this->level_scanners_[level_i];
      KVSlice* result = nullptr;
      for (;;) {
        Optional<EditSlice> slice = level_scanner.next();
        if (!slice) {
          this->deactivate(level_i);
          return KVSlice{};
        }

        batt::case_of(
            *slice,
            [](Slice<const EditView>&) {
              BATT_PANIC() << "Invalid EditSlice type: EditView";
              BATT_UNREACHABLE();
            },
            [&](Slice<const PackedKeyValue>& kv_slice) {
              if (!kv_slice.empty()) {
                result = &kv_slice;
              }
            });

        if (result) {
          return *result;
        }
      }
    }

    void deactivate(i32 level_i)
    {
      this->active_levels_ &= ~(u64{1} << level_i);
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status validate_page_layout(i32 height, const llfs::PinnedPage& pinned_page)
  {
    const auto& page_header =
        *static_cast<const llfs::PackedPageHeader*>(pinned_page->const_buffer().data());

    if (height > 1) {
      if (page_header.layout_id != NodePageView::page_layout_id()) {
        return {batt::StatusCode::kDataLoss};
      }
    } else {
      BATT_CHECK_EQ(height, 1);
      if (page_header.layout_id != LeafPageView::page_layout_id()) {
        return {batt::StatusCode::kDataLoss};
      }
    }

    return OkStatus();
  }

  Status enter_subtree(i32 subtree_height, llfs::PageIdSlot subtree_root)
  {
    for (;;) {
      StatusOr<llfs::PinnedPage> pinned_page =
          subtree_root.load_through(this->page_loader_,
                                    llfs::PageLoadOptions{
                                        llfs::PinPageToJob::kFalse,
                                        llfs::OkIfNotFound{false},
                                        llfs::LruPriority{kNodeLruPriority},
                                    });

      BATT_REQUIRE_OK(pinned_page);
      BATT_REQUIRE_OK(this->validate_page_layout(subtree_height, *pinned_page));

      if (subtree_height == 1) {
        BATT_REQUIRE_OK(this->enter_leaf(std::move(*pinned_page)));
        break;
      }

      BATT_REQUIRE_OK(this->enter_node(std::move(*pinned_page)));

      PathFrame& frame = this->path_frames_.back();

      subtree_root =
          llfs::PageIdSlot::from_page_id(frame.node_->get_child_id(frame.pivot_i_).unpack());

      --subtree_height;
    }

    return OkStatus();
  }

  Status enter_leaf(llfs::PinnedPage&& pinned_page)
  {
    const PackedLeafPage& leaf = PackedLeafPage::view_of(pinned_page);
    this->path_frames_.emplace_back(*this, std::move(pinned_page), leaf, this->min_key_);
    return OkStatus();
  }

  Status enter_node(llfs::PinnedPage&& pinned_page)
  {
    const PackedNodePage& node = PackedNodePage::view_of(pinned_page);
    this->path_frames_.emplace_back(*this, std::move(pinned_page), node, this->min_key_);
    return OkStatus();
  }

  Status resume()
  {
    this->needs_resume_ = false;

    for (;;) {
      if (this->path_frames_.empty()) {
        break;
      }

      PathFrame& frame = this->path_frames_.back();

      if (frame.node_) {
        if (frame.pivot_i_ < frame.node_->pivot_count()) {
          ++frame.pivot_i_;
          if (frame.pivot_i_ != frame.node_->pivot_count()) {
            BATT_REQUIRE_OK(
                this->enter_subtree(frame.get_height() - 1,
                                    llfs::PageIdSlot::from_page_id(
                                        frame.node_->get_child_id(frame.pivot_i_).unpack())));
            continue;
          }
        }
      }

      if (frame.active_levels_ != 0) {
        break;
      }

      this->path_frames_.pop_back();
    }

    return OkStatus();
  }

  Status set_next_item()
  {
    for (;;) {
      if (this->heap_.empty()) {
        return OkStatus();
      }

      KVSlice* slice = this->heap_front();
      BATT_CHECK(!slice->empty());

      if (!this->next_item_) {
        this->pop_heap(slice);
        this->next_item_.emplace(to_edit_view(slice->front()));
        slice->drop_front();
        this->push_heap(slice);
        continue;
      }

      if (get_key(*this->next_item_) == get_key(slice->front())) {
        this->pop_heap(slice);
        if (this->next_item_->needs_combine()) {
          *this->next_item_ = combine(*this->next_item_, to_edit_view(slice->front()));
        }
        slice->drop_front();
        this->push_heap(slice);
        continue;
      }

      break;
    }

    if (this->needs_resume_) {
      BATT_REQUIRE_OK(this->resume());
    }

    return OkStatus();
  }

  KVSlice* heap_front()
  {
    return this->heap_.front();
  }

  void pop_heap(KVSlice* expected_front)
  {
    BATT_CHECK_EQ(expected_front, this->heap_.front());
    std::pop_heap(this->heap_.begin(), this->heap_.end(), KVSliceMaxHeapOrder{});
    this->heap_.pop_back();
  }

  bool push_heap(KVSlice* slice)
  {
    if (slice->empty()) {
      PathFrameLocation& location = this->frame_for_tier_[slice - this->tiers_.data()];
      *slice = location.frame->pull_next(location.level_i);
      if (slice->empty()) {
        this->needs_resume_ = true;
        return false;
      }
    }
    this->heap_.push_back(slice);
    std::push_heap(this->heap_.begin(), this->heap_.end(), KVSliceMaxHeapOrder{});
    return true;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageLoader& page_loader_;
  llfs::PageIdSlot root_;
  i32 tree_height_;
  KeyView min_key_;
  bool needs_resume_ = false;
  Optional<EditView> next_item_;
  Status status_;
  boost::container::static_vector<PathFrame, kMaxTreeHeight - 1> path_frames_;
  boost::container::static_vector<PathFrameLocation, kMaxHeapSize> frame_for_tier_;
  boost::container::static_vector<KVSlice, kMaxHeapSize> tiers_;
  boost::container::static_vector<KVSlice*, kMaxHeapSize> heap_;
};

#endif

}  // namespace turtle_kv
