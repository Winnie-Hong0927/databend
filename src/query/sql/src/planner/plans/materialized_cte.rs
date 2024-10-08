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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;

use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::ColumnBinding;
use crate::IndexType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedCte {
    pub(crate) cte_idx: IndexType,
    pub(crate) materialized_output_columns: Vec<ColumnBinding>,
    pub(crate) materialized_indexes: HashMap<IndexType, IndexType>,
}

impl Operator for MaterializedCte {
    fn rel_op(&self) -> RelOp {
        RelOp::MaterializedCte
    }

    fn arity(&self) -> usize {
        2
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;

        let output_columns = left_prop.output_columns.clone();
        let outer_columns = left_prop.outer_columns.clone();
        let used_columns = left_prop.used_columns.clone();
        let orderings = left_prop.orderings.clone();

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings,
            partition_orderings: None,
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Serial,
        })
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let right_stat_info = rel_expr.derive_cardinality_child(1)?;
        Ok(Arc::new(StatInfo {
            cardinality: right_stat_info.cardinality,
            statistics: right_stat_info.statistics.clone(),
        }))
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        // Todo(xudong): consider cluster for materialized cte
        Ok(RequiredProperty {
            distribution: Distribution::Serial,
        })
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        Ok(vec![vec![
            RequiredProperty {
                distribution: Distribution::Serial,
            },
            RequiredProperty {
                distribution: Distribution::Serial,
            },
        ]])
    }
}

impl std::hash::Hash for MaterializedCte {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.cte_idx.hash(state);
        self.materialized_output_columns.hash(state);
    }
}
