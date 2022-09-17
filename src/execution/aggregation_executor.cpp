//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // build the hash table (pipeline breaker)
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    AggregateKey agg_key = MakeAggregateKey(&tuple);
    AggregateValue agg_val = MakeAggregateValue(&tuple);
    aht_.InsertCombine(agg_key, agg_val);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  AggregateKey key;
  AggregateValue value;
  while (aht_iterator_ != aht_.End()) {
    key = aht_iterator_.Key();
    value = aht_iterator_.Val();
    ++aht_iterator_;
    const AbstractExpression *having = plan_->GetHaving();
    if (having == nullptr || having->EvaluateAggregate(key.group_bys_, value.aggregates_).GetAs<bool>()) {
      std::vector<Value> values;
      for (auto &col : plan_->OutputSchema()->GetColumns()) {
        values.push_back(col.GetExpr()->EvaluateAggregate(key.group_bys_, value.aggregates_));
      }
      *tuple = Tuple(values, GetOutputSchema());
      return true;
    }
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
