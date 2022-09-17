//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(move(left_executor)),
      right_executor_(move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  non_empty_ = left_executor_->Next(&left_tuple_, &left_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!non_empty_) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;

  auto out_schema = plan_->OutputSchema();
  const AbstractExpression *predicate = plan_->Predicate();
  while (true) {
    // only call next on the left executor when right is done, return when left executor ends
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (!(left_executor_->Next(&left_tuple_, &left_rid_))) {
        non_empty_ = false;
        return false;
      }
      // check if right has no tuples at all
      right_executor_->Init();
      if (!right_executor_->Next(&right_tuple, &right_rid)) {
        return false;
      }
    }
    // Evaluate
    if (predicate == nullptr || predicate
                                    ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema())
                                    .GetAs<bool>()) {
      size_t cols = out_schema->GetColumnCount();
      std::vector<Value> values(cols);
      for (size_t i = 0; i < cols; i++) {
        values[i] = out_schema->GetColumn(i).GetExpr()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                                     &right_tuple, right_executor_->GetOutputSchema());
      }
      *tuple = Tuple(values, out_schema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
