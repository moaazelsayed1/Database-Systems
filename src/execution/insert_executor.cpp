//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_oid_t table_id = plan_->TableOid();

  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_id);
}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (plan_->IsRawInsert()) {
    auto values = plan_->RawValues();
    for (const auto &value : values) {
      Tuple tmp_tuple(value, &table_info_->schema_);
      Insert(&tmp_tuple);
    }
  } else {
    Tuple tmp_tuple;
    RID tmp_rid;
    child_executor_->Init();
    while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
      Insert(&tmp_tuple);
    }
  }
  return false;
}

void InsertExecutor::Insert(Tuple *tuple) {
  RID rid;
  table_info_->table_->InsertTuple(*tuple, &rid, exec_ctx_->GetTransaction());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto index : indexes) {
    Tuple idx_tuple = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    index->index_->InsertEntry(idx_tuple, rid, exec_ctx_->GetTransaction());
  }
}
}  // namespace bustub
