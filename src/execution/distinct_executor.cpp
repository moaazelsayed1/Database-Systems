//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  hash_table_.clear();
}

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    DistinctKey key;
    size_t cols = plan_->OutputSchema()->GetColumnCount();
    key.distinct_.reserve(cols);
    for (uint32_t i = 0; i < cols; i++) {
      key.distinct_.push_back(tuple->GetValue(plan_->OutputSchema(), i));
    }
    if (hash_table_.count(key) <= 0) {
      hash_table_.insert(std::move(key));
      return true;
    }
  }
  return false;
}
}  // namespace bustub
