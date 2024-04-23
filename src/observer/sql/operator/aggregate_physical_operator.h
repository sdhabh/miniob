#pragma once 

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include "sql/expr/tuple.h"

/**
 * @brief 物理算子
 * @defgroup PhysicalOperator
 */

 class Trx;

 class AggregatePhysicalOperator : public PhysicalOperator 
{
public :
 AggregatePhysicalOperator (){}

 virtual ~AggregatePhysicalOperator ()= default ;

  void add_aggregation (const AggrOp aggregation);

 PhysicalOperatorType type () const override 
 {
  return PhysicalOperatorType :: AGGREGATE ;
 }

 RC open ( Trx *trx ) override ;
 RC next () override ;
 RC close () override ;

 Tuple * current_tuple () override 
 {
    LOG_TRACE("return result tuple");
    return &result_tuple_;
 }

 private :
 std::vector<AggrOp>aggregations_;
  ValueListTuple result_tuple_;
 // Trx   *trx_   = nullptr;
 };
