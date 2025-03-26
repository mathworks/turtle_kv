#include <turtle_kv/tree/batch_update.hpp>
//

namespace turtle_kv {

using TrimResult = BatchUpdate::TrimResult;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BatchUpdate BatchUpdate::make_child_update() const noexcept
{
  return BatchUpdate{
      .worker_pool = this->worker_pool,
      .page_loader = this->page_loader,
      .cancel_token = this->cancel_token,
      .result_set = MergeCompactor::ResultSet</*kDecayToItems=*/false>{},
      .edit_size_totals = None,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void BatchUpdate::update_edit_size_totals() noexcept
{
  this->edit_size_totals.emplace(compute_running_total(this->worker_pool, this->result_set));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TrimResult BatchUpdate::trim_back_down_to_size(usize byte_size_limit) noexcept
{
  TrimResult result;

  if (!this->edit_size_totals) {
    this->update_edit_size_totals();
  }

  const usize orig_byte_size = this->edit_size_totals->back() - this->edit_size_totals->front();
  if (orig_byte_size <= byte_size_limit) {
    return result;
  }

  const usize orig_edit_count = this->result_set.size();

  usize new_byte_size = orig_byte_size;
  usize new_edit_count =                                                        //
      std::distance(this->edit_size_totals->begin(),                            //
                    std::lower_bound(this->edit_size_totals->begin(),           //
                                     std::prev(this->edit_size_totals->end()),  //
                                     byte_size_limit));

  BATT_CHECK_LE(new_edit_count, this->result_set.size());

  while (new_edit_count > 0) {
    new_byte_size = (*this->edit_size_totals)[new_edit_count] - this->edit_size_totals->front();
    if (new_byte_size <= byte_size_limit) {
      while (new_edit_count + 1 < this->edit_size_totals->size()) {
        const usize next_edit_size = (*this->edit_size_totals)[new_edit_count + 1] -
                                     (*this->edit_size_totals)[new_edit_count];
        if (new_byte_size + next_edit_size > byte_size_limit) {
          break;
        }
        new_byte_size += next_edit_size;
        new_edit_count += 1;
      }
      break;
    }

    BATT_CHECK_GT(new_edit_count, 0);
    --new_edit_count;
  }

  this->result_set.drop_after_n(new_edit_count);
  this->edit_size_totals->set_size(new_edit_count + 1);

  BATT_CHECK_EQ(this->result_set.size(), new_edit_count);
  BATT_CHECK_EQ(this->edit_size_totals->size(), new_edit_count + 1);
  BATT_CHECK_EQ(this->edit_size_totals->back() - this->edit_size_totals->front(), new_byte_size);
  BATT_CHECK_GE(orig_edit_count, new_edit_count);
  BATT_CHECK_GE(orig_byte_size, new_byte_size);

  result.n_items_trimmed = orig_edit_count - new_edit_count;
  result.n_bytes_trimmed = orig_byte_size - new_byte_size;

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const BatchUpdate::TrimResult& t)
{
  return out << "TrimResult{.n_items_trimmed=" << t.n_items_trimmed
             << ", .n_bytes_trimmed=" << t.n_bytes_trimmed << ",}";
}

}  // namespace turtle_kv
