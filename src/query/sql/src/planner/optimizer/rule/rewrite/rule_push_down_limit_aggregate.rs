// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::Limit;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::Sort;
use crate::plans::SortItem;

/// Input:  Limit | Sort
///           \
///          Aggregate
///             \
///              *
///
/// Output: Limit | Sort
///           \
///          Aggregate(padding limit | rank_limit)
///             \
///               *
pub struct RulePushDownRankLimitAggregate {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownRankLimitAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::RulePushDownRankLimitAggregate,
            matchers: vec![
                Matcher::MatchOp {
                    op_type: RelOp::Limit,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Aggregate,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Aggregate,
                            children: vec![Matcher::Leaf],
                        }],
                    }],
                },
                Matcher::MatchOp {
                    op_type: RelOp::Sort,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Aggregate,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Aggregate,
                            children: vec![Matcher::Leaf],
                        }],
                    }],
                },
                Matcher::MatchOp {
                    op_type: RelOp::Sort,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::EvalScalar,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Aggregate,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::Aggregate,
                                children: vec![Matcher::Leaf],
                            }],
                        }],
                    }],
                },
            ],
        }
    }

    // There is no order by, so we don't care the order of result.
    // To make query works with consistent result and more efficient, we will inject a order by before limit
    fn apply_limit(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        if let Some(mut count) = limit.limit {
            count += limit.offset;
            let agg_final = s_expr.child(0)?;
            let agg_partial = agg_final.child(0)?;

            let mut agg_limit: Aggregate = agg_partial.plan().clone().try_into()?;

            let sort_items = agg_limit
                .group_items
                .iter()
                .map(|g| SortItem {
                    index: g.index,
                    asc: true,
                    nulls_first: false,
                })
                .collect::<Vec<_>>();
            agg_limit.rank_limit = Some((sort_items.clone(), count));

            let sort = Sort {
                items: sort_items.clone(),
                limit: Some(count),
                after_exchange: None,
                pre_projection: None,
                window_partition: vec![],
            };

            let agg_partial = SExpr::create_unary(
                Arc::new(RelOperator::Aggregate(agg_limit)),
                Arc::new(agg_partial.child(0)?.clone()),
            );
            let agg_final = agg_final.replace_children(vec![agg_partial.into()]);
            let sort = SExpr::create_unary(Arc::new(RelOperator::Sort(sort)), agg_final.into());
            let mut result = s_expr.replace_children(vec![Arc::new(sort)]);

            result.set_applied_rule(&self.id);
            state.add_result(result);
        }
        Ok(())
    }

    fn apply_sort(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let sort: Sort = s_expr.plan().clone().try_into()?;
        let mut has_eval_scalar = false;
        let agg = match s_expr.child(0)?.plan().rel_op() {
            RelOp::Aggregate => s_expr.child(0)?,
            RelOp::EvalScalar => {
                has_eval_scalar = true;
                s_expr.child(0)?.child(0)?
            }
            _ => return Ok(()),
        };

        let agg_limit_expr = agg.child(0)?;
        let mut agg_limit: Aggregate = agg_limit_expr.plan().clone().try_into()?;

        if agg_limit.mode != AggregateMode::Partial {
            return Ok(());
        }

        if let Some(limit) = sort.limit {
            let is_order_subset = sort
                .items
                .iter()
                .all(|k| agg_limit.group_items.iter().any(|g| g.index == k.index));

            if !is_order_subset {
                return Ok(());
            }
            let mut sort_items = Vec::with_capacity(agg_limit.group_items.len());
            let mut not_found_sort_items = vec![];
            for i in 0..agg_limit.group_items.len() {
                let group_item = &agg_limit.group_items[i];
                if let Some(sort_item) = sort.items.iter().find(|k| k.index == group_item.index) {
                    sort_items.push(SortItem {
                        index: group_item.index,
                        asc: sort_item.asc,
                        nulls_first: sort_item.nulls_first,
                    });
                } else {
                    not_found_sort_items.push(SortItem {
                        index: group_item.index,
                        asc: true,
                        nulls_first: false,
                    });
                }
            }
            sort_items.extend(not_found_sort_items);

            agg_limit.rank_limit = Some((sort_items, limit));

            let agg_partial = SExpr::create_unary(
                Arc::new(RelOperator::Aggregate(agg_limit)),
                Arc::new(agg_limit_expr.child(0)?.clone()),
            );

            let agg = agg.replace_children(vec![Arc::new(agg_partial)]);
            let mut result = if has_eval_scalar {
                let eval_scalar = s_expr.child(0)?.replace_children(vec![Arc::new(agg)]);
                s_expr.replace_children(vec![Arc::new(eval_scalar)])
            } else {
                s_expr.replace_children(vec![Arc::new(agg)])
            };
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }
        Ok(())
    }
}

impl Rule for RulePushDownRankLimitAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        match s_expr.plan().rel_op() {
            RelOp::Limit => self.apply_limit(s_expr, state),
            RelOp::Sort | RelOp::EvalScalar => self.apply_sort(s_expr, state),
            _ => Ok(()),
        }
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
