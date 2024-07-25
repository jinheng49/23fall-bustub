#include "execution/execution_common.h"
#include <_types/_uint32_t.h>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // UNIMPLEMENTED("not implemented");
  if(base_meta.is_deleted_ && undo_logs.empty()){
    return std::nullopt;
  }
  Tuple res_tuple = base_tuple;
  bool is_deleted = false;
  for(const auto &undo_log : undo_logs){
    if(undo_log.is_deleted_){
      is_deleted = true;
    }else{
      is_deleted = false;
      Schema undo_schema = GetUndoLogSchema(undo_log,schema);
      std::vector<Value> values;
      uint32_t tuple_sz = schema->GetColumnCount();
      values.reserve(tuple_sz);
      for(uint32_t i = 0,j = 0; i < tuple_sz; i++){
        if(undo_log.modified_fields_[i]){
          values.push_back(undo_log.tuple_.GetValue(&undo_schema,j++));
        }else{
          values.push_back(res_tuple.GetValue(schema, i)) ;
        }
      }
      res_tuple = Tuple(std::move(values), schema);
    }
  }
  if(is_deleted){
    return std::nullopt;
  }
  return res_tuple;

}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub

auto GetUndoLogSchema(const bustub::UndoLog &undo_log, const bustub::Schema *schema) {
  // UNIMPLEMENTED("not implemented");
  std::vector<uint32_t> cols;
  uint32_t tuple_sz = schema->GetColumnCount();
  for(uint32_t i = 0; i < tuple_sz; i++){
    if(undo_log.modified_fields_[i]){
      cols.push_back(i);
    }
  }
  return bustub::Schema::CopySchema(schema, cols);
}