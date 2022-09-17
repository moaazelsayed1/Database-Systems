//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan->TableOid());
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  RID tmp_rid;
  Tuple tmp_tuple;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    table_info_->table_->MarkDelete(tmp_rid, exec_ctx_->GetTransaction());
    for (auto index : indexes) {
      Tuple idx_tmp_tuple =
          tmp_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(idx_tmp_tuple, tmp_rid, exec_ctx_->GetTransaction());
    }
  }
  return false;
}
}  // namespace bustub
