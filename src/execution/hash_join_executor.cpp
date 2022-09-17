//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <utility>
#include <vector>
#include "execution/expressions/abstract_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  // build the hash table (pipeline breaker)
  while (left_executor_->Next(&tuple, &rid)) {
    HashJoinKey join_key;
    join_key.join_key_ = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_executor_->GetOutputSchema());
    if (hash_table_.count(join_key) != 0) {
      hash_table_[join_key].push_back(tuple);
    } else {
      hash_table_.insert({join_key, std::vector{tuple}});
    }
  }
  RID right_rid;
  non_empty_ = right_executor_->Next(&right_tuple_, &right_rid);
  next_pos_ = 0;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (hash_table_.empty() || !non_empty_) {
    return false;
  }
  RID right_rid;
  HashJoinKey join_key;

  while (true) {
    join_key.join_key_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_executor_->GetOutputSchema());

    if (hash_table_.count(join_key) != 0) {
      std::vector<bustub::Tuple> left_tuples_vec = hash_table_.find(join_key)->second;
      if (next_pos_ < left_tuples_vec.size()) {
        Tuple left_tuple = left_tuples_vec[next_pos_];
        next_pos_++;
        std::vector<Value> values;
        for (const auto &value : GetOutputSchema()->GetColumns()) {
          values.push_back(value.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple_,
                                                         right_executor_->GetOutputSchema()));
        }
        *tuple = Tuple(values, GetOutputSchema());
        return true;
      }
    }
    if (!right_executor_->Next(&right_tuple_, &right_rid)) {
      non_empty_ = false;
      return false;
    }
    next_pos_ = 0;
  }
  return false;
}

}  // namespace bustub
