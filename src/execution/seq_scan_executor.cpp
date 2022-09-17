//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <vector>
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), itr_(nullptr, RID{}, nullptr) {
  table_oid_t table_id = plan_->GetTableOid();

  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_id);
}

void SeqScanExecutor::Init() { itr_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (itr_ != table_info_->table_->End()) {
    const Schema *out_schema = plan_->OutputSchema();
    size_t cols = out_schema->GetColumnCount();
    std::vector<Value> values(cols);

    for (size_t i = 0; i < cols; i++) {
      values[i] = out_schema->GetColumn(i).GetExpr()->Evaluate(&(*itr_), &(table_info_->schema_));
    }
    RID tmp_rid = itr_->GetRid();
    itr_++;
    Tuple tmp_tuple(values, out_schema);
    const AbstractExpression *predicate = plan_->GetPredicate();
    if (predicate == nullptr || predicate->Evaluate(&tmp_tuple, out_schema).GetAs<bool>()) {
      *rid = tmp_rid;
      *tuple = tmp_tuple;
      return true;
    }
  }
  return false;
}
}  // namespace bustub
