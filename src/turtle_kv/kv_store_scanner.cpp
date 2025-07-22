#include <turtle_kv/kv_store_scanner.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::KVStoreScanner(KVStore& kv_store, const KeyView& min_key) noexcept
    : pinned_state_{kv_store.state_.load()}
    , page_loader_{kv_store.per_thread_.get(&kv_store).get_page_loader()}
    , root_{this->pinned_state_->base_checkpoint_.tree()->page_id_slot_or_panic()}
    , tree_height_{this->pinned_state_->base_checkpoint_.tree_height()}
    , min_key_{min_key}
    , needs_resume_{false}
    , next_item_{None}
    , status_{OkStatus()}
    , mem_table_scanner_{this->pinned_state_->mem_table_->ordered_index(), min_key}
    , delta_storage_{this->static_delta_storage_.data()}
    , tree_scan_path_{}
    , scan_levels_{}
    , heap_{}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::KVStoreScanner(llfs::PageLoader& page_loader,
                                            const llfs::PageIdSlot& root,
                                            i32 tree_height,
                                            const KeyView& min_key) noexcept
    : pinned_state_{nullptr}
    , page_loader_{page_loader}
    , root_{root}
    , tree_height_{tree_height}
    , min_key_{min_key}
    , needs_resume_{false}
    , next_item_{None}
    , status_{OkStatus()}
    , mem_table_scanner_{None}
    , delta_storage_{this->static_delta_storage_.data()}
    , tree_scan_path_{}
    , scan_levels_{}
    , heap_{}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KVStoreScanner::~KVStoreScanner() noexcept
{
  if (this->delta_storage_ != this->static_delta_storage_.data()) {
    delete[] this->delta_storage_;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::start()
{
  if (this->pinned_state_) {
    const usize n_deltas = this->pinned_state_->deltas_.size();

    // Reserve space for MemTable (active + deltas) in ScanLevels.
    //
    this->scan_levels_.reserve(1 + n_deltas + kMaxHeapSize);

    // Create the active MemTable scanner.
    //
    BATT_CHECK(this->mem_table_scanner_);
    if (!this->mem_table_scanner_->is_done()) {
      this->scan_levels_.emplace_back(ActiveMemTableTag{},
                                      *this->pinned_state_->mem_table_,
                                      *this->mem_table_scanner_);
    }

    // Reserve space for delta MemTable scanners.
    //
    if (n_deltas > this->static_delta_storage_.size()) {
      this->delta_storage_ = new DeltaMemTableScannerStorage[n_deltas];
    }

    // Create scanners for delta MemTables.
    //
    {
      DeltaMemTableScannerStorage* p_mem = this->delta_storage_;
      for (usize delta_i = n_deltas; delta_i > 0;) {
        --delta_i;

        MemTable& delta_mem_table = *this->pinned_state_->deltas_[delta_i];

        Optional<Slice<const EditView>> compacted = delta_mem_table.poll_compacted_edits();
        if (compacted) {
          const EditView* last = compacted->end();
          const EditView* first =
              std::lower_bound(compacted->begin(), last, this->min_key_, KeyOrder{});
          if (first != last) {
            this->scan_levels_.emplace_back(Slice<const EditView>{first, last});
          }
          continue;
        }

        auto& art_scanner = *(new (p_mem) ART<void>::Scanner<ARTBase::Synchronized::kFalse>{
            delta_mem_table.ordered_index(),
            this->min_key_,
        });
        ++p_mem;

        if (!art_scanner.is_done()) {
          this->scan_levels_.emplace_back(DeltaMemTableTag{}, delta_mem_table, art_scanner);
        }
      }
    }
  }
  // (ART<void>::Scanner::~Scanner() has no side-effects, so just skip calling destructors)

  // Initialize a path down the checkpoint tree (unless empty).
  //
  if (this->root_.is_valid()) {
    BATT_REQUIRE_OK(this->enter_subtree(this->tree_height_, this->root_, std::false_type{}));
    BATT_REQUIRE_OK(this->resume());
  }

  // Run make heap once at the beginning.
  //
  this->heap_.reset(as_slice(this->scan_levels_), /*minimum_capacity=*/kMaxHeapSize);

  BATT_REQUIRE_OK(this->set_next_item());

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto KVStoreScanner::peek() -> const Optional<Item>&
{
  return this->next_item_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto KVStoreScanner::next() -> Optional<Item>
{
  Optional<Item> item;
  std::swap(item, this->next_item_);
  if (item) {
    this->status_.Update(this->set_next_item());
  }
  return item;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::status() const
{
  return this->status_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> KVStoreScanner::read(const Slice<std::pair<KeyView, ValueView>>& buffer)
{
  usize n_read = 0;

  for (; n_read != buffer.size(); ++n_read) {
    if (!this->next_item_) {
      break;
    }

    buffer[n_read].first = this->next_item_->key;
    buffer[n_read].second = this->next_item_->value;

    this->next_item_ = None;
    BATT_REQUIRE_OK(this->set_next_item());
  }

  return n_read;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::validate_page_layout(i32 height, const llfs::PinnedPage& pinned_page)
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename InsertHeapBool>
Status KVStoreScanner::enter_subtree(i32 subtree_height,
                                     llfs::PageIdSlot subtree_root,
                                     InsertHeapBool insert_heap)
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

    NodeScanState& node_state = this->tree_scan_path_.back();

    subtree_root = llfs::PageIdSlot::from_page_id(
        node_state.node_->get_child_id(node_state.pivot_i_).unpack());

    --subtree_height;
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename InsertHeapBool>
Status KVStoreScanner::enter_leaf(llfs::PinnedPage&& pinned_page, InsertHeapBool insert_heap)
{
  const PackedLeafPage& leaf = PackedLeafPage::view_of(pinned_page);
  this->tree_scan_path_.emplace_back(*this, std::move(pinned_page), leaf, insert_heap);
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename InsertHeapBool>
Status KVStoreScanner::enter_node(llfs::PinnedPage&& pinned_page, InsertHeapBool insert_heap)
{
  const PackedNodePage& node = PackedNodePage::view_of(pinned_page);
  this->tree_scan_path_.emplace_back(*this, std::move(pinned_page), node, insert_heap);
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::resume()
{
  this->needs_resume_ = false;

  for (;;) {
    if (this->tree_scan_path_.empty()) {
      break;
    }

    NodeScanState& node_state = this->tree_scan_path_.back();

    if (node_state.node_) {
      if (node_state.pivot_i_ < node_state.node_->pivot_count()) {
        ++node_state.pivot_i_;
        if (node_state.pivot_i_ != node_state.node_->pivot_count()) {
          BATT_REQUIRE_OK(
              this->enter_subtree(node_state.get_height() - 1,
                                  llfs::PageIdSlot::from_page_id(
                                      node_state.node_->get_child_id(node_state.pivot_i_).unpack()),
                                  /*insert_heap=*/std::true_type{}));
          continue;
        }
      }
    }

    if (node_state.active_levels_ != 0) {
      break;
    }

    this->tree_scan_path_.pop_back();
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreScanner::set_next_item()
{
  for (;;) {
    if (this->heap_.empty()) {
      return OkStatus();
    }

    ScanLevel* scan_level = this->heap_.first();

    if (!this->next_item_) {
      this->next_item_.emplace(scan_level->item());

    } else if (this->next_item_->key == scan_level->key) {
      if (this->next_item_->needs_combine()) {
        this->next_item_->value = combine(this->next_item_->value, scan_level->value());
      }

    } else {
      break;
    }

    if (scan_level->advance()) {
      this->heap_.update_first();
    } else {
      this->heap_.remove_first();
      this->needs_resume_ = true;
    }
  }

  if (this->needs_resume_) {
    BATT_REQUIRE_OK(this->resume());
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(const KVSlice& kv_slice,
                                                  NodeScanState* node_state,
                                                  i32 buffer_level_i) noexcept
    : key{get_key(kv_slice.front())}
    , state_impl{TreeLevelScanState{
          .kv_slice = kv_slice,
          .node_state = node_state,
          .buffer_level_i = buffer_level_i,
      }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(
    ActiveMemTableTag,
    MemTable& mem_table,
    ART<void>::Scanner<ARTBase::Synchronized::kTrue>& art_scanner) noexcept
    : key{art_scanner.get_key()}
    , state_impl{MemTableScanState<ARTBase::Synchronized::kTrue>{
          .mem_table_ = &mem_table,
          .art_scanner_ = &art_scanner,
      }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(
    DeltaMemTableTag,
    MemTable& mem_table,
    ART<void>::Scanner<ARTBase::Synchronized::kFalse>& art_scanner) noexcept
    : key{art_scanner.get_key()}
    , state_impl{MemTableScanState<ARTBase::Synchronized::kFalse>{
          .mem_table_ = &mem_table,
          .art_scanner_ = &art_scanner,
      }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreScanner::ScanLevel::ScanLevel(
    const Slice<const EditView>& edit_view_slice) noexcept
    : key{edit_view_slice.front().key}
    , state_impl{edit_view_slice}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
EditView KVStoreScanner::ScanLevel::item() const
{
  return batt::case_of(
      this->state_impl,
      [](NoneType) -> EditView {
        BATT_PANIC() << "illegal state";
        BATT_UNREACHABLE();
      },
      [this](const MemTableScanState<ARTBase::Synchronized::kTrue>& state) -> EditView {
        MemTableEntry entry;
        const bool found = state.mem_table_->hash_index().find_key(this->key, entry);
        BATT_CHECK(found);

        return EditView{entry.key_, entry.value_};
      },
      [this](const MemTableScanState<ARTBase::Synchronized::kFalse>& state) -> EditView {
        const MemTableEntry* entry = state.mem_table_->hash_index().unsynchronized_find_key(key);
        BATT_CHECK_NOT_NULLPTR(entry);

        return EditView{entry->key_, entry->value_};
      },
      [](const Slice<const EditView>& state) -> EditView {
        return state.front();
      },
      [this](const TreeLevelScanState& state) -> EditView {
        return EditView{this->key, get_value(state.kv_slice.front())};
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ValueView KVStoreScanner::ScanLevel::value() const
{
  return batt::case_of(
      this->state_impl,
      [](NoneType) -> ValueView {
        BATT_PANIC() << "illegal state";
        BATT_UNREACHABLE();
      },
      [this](const MemTableScanState<ARTBase::Synchronized::kTrue>& state) -> ValueView {
        return state.mem_table_->get(this->key).value_or_panic();
      },
      [this](const MemTableScanState<ARTBase::Synchronized::kFalse>& state) -> ValueView {
        return state.mem_table_->finalized_get(this->key).value_or_panic();
      },
      [](const Slice<const EditView>& state) -> ValueView {
        return state.front().value;
      },
      [](const TreeLevelScanState& state) -> ValueView {
        return get_value(state.kv_slice.front());
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool KVStoreScanner::ScanLevel::advance()
{
  return batt::case_of(
      this->state_impl,
      [](NoneType) -> bool {
        BATT_PANIC() << "illegal state";
        BATT_UNREACHABLE();
      },
      [this](MemTableScanState<ARTBase::Synchronized::kTrue>& state) -> bool {
        state.art_scanner_->advance();
        if (state.art_scanner_->is_done()) {
          return false;
        }
        this->key = state.art_scanner_->get_key();
        return true;
      },
      [this](MemTableScanState<ARTBase::Synchronized::kFalse>& state) -> bool {
        state.art_scanner_->advance();
        if (state.art_scanner_->is_done()) {
          return false;
        }
        this->key = state.art_scanner_->get_key();
        return true;
      },
      [this](Slice<const EditView>& state) -> bool {
        state.drop_front();
        if (state.empty()) {
          return false;
        }
        this->key = state.front().key;
        return true;
      },
      [this](TreeLevelScanState& state) -> bool {
        state.kv_slice.drop_front();
        if (state.kv_slice.empty()) {
          state.kv_slice = state.node_state->pull_next(state.buffer_level_i);
          if (state.kv_slice.empty()) {
            return false;
          }
        }
        this->key = get_key(state.kv_slice.front());
        return true;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kInsertHeap>
/*explicit*/ KVStoreScanner::NodeScanState::NodeScanState(
    KVStoreScanner& kv_scanner,
    llfs::PinnedPage&& page,
    const PackedNodePage& node,
    std::integral_constant<bool, kInsertHeap>) noexcept
    : active_levels_{0}
    , pinned_page_{std::move(page)}
    , node_{&node}
    , pivot_i_{(i32)in_node(node).find_pivot_containing(kv_scanner.min_key_)}
{
  const i32 n_levels = this->node_->get_level_count();

  for (i32 buffer_level_i = 0; buffer_level_i < n_levels; ++buffer_level_i) {
    PackedLevel& level = this->levels_.emplace_back(this->node_->is_size_tiered()
                                                        ? this->node_->get_tier(buffer_level_i)
                                                        : this->node_->get_level(buffer_level_i));

    this->level_scanners_.emplace_back(*this->node_,
                                       level,
                                       kv_scanner.page_loader_,
                                       llfs::PinPageToJob::kFalse,
                                       this->pivot_i_,
                                       kv_scanner.min_key_);

    KVSlice first_slice = this->pull_next(buffer_level_i);

    if (!first_slice.empty()) {
      this->active_levels_ |= (u64{1} << buffer_level_i);
      ScanLevel& level = kv_scanner.scan_levels_.emplace_back(first_slice, this, buffer_level_i);
      if (kInsertHeap) {
        kv_scanner.heap_.insert(&level);
      }
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kInsertHeap>
/*explicit*/ KVStoreScanner::NodeScanState::NodeScanState(
    KVStoreScanner& kv_scanner,
    llfs::PinnedPage&& page,
    const PackedLeafPage& leaf,
    std::integral_constant<bool, kInsertHeap>) noexcept
    : active_levels_{0}
    , pinned_page_{std::move(page)}
    , node_{nullptr}
    , pivot_i_{0}
{
  KVSlice first_slice = as_slice(leaf.lower_bound(kv_scanner.min_key_), leaf.items_end());
  if (first_slice.empty()) {
    return;
  }

  this->active_levels_ = 1;
  ScanLevel& level = kv_scanner.scan_levels_.emplace_back(first_slice, this, 0);
  if (kInsertHeap) {
    kv_scanner.heap_.insert(&level);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i32 KVStoreScanner::NodeScanState::get_height() const
{
  if (!this->node_) {
    return 1;
  }
  return this->node_->height;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto KVStoreScanner::NodeScanState::pull_next(i32 buffer_level_i) -> KVSlice
{
  if (!this->node_) {
    this->deactivate(buffer_level_i);
    return KVSlice{};
  }

  PackedLevelScanner& level_scanner = this->level_scanners_[buffer_level_i];
  KVSlice* result = nullptr;
  for (;;) {
    Optional<EditSlice> slice = level_scanner.next();
    if (!slice) {
      this->deactivate(buffer_level_i);
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStoreScanner::NodeScanState::deactivate(i32 buffer_level_i)
{
  this->active_levels_ &= ~(u64{1} << buffer_level_i);
}

}  // namespace turtle_kv
