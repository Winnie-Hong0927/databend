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

use std::any::Any;
use std::sync::Arc;

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;

use crate::sessions::TableContext;
use crate::table_functions::parse_db_tb_args;
use crate::table_functions::string_literal;
use crate::table_functions::ClusteringStatistics;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;
use crate::FuseTable;
use crate::Table;

const FUSE_FUNC_CLUSTERING_STAT: &str = "clustering_statistics";

pub struct ClusteringStatisticsTable {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
}

impl ClusteringStatisticsTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let (arg_database_name, arg_table_name) =
            parse_db_tb_args(&table_args, FUSE_FUNC_CLUSTERING_STAT)?;

        let engine = FUSE_FUNC_CLUSTERING_STAT.to_owned();

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: ClusteringStatistics::schema(),
                engine,
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(ClusteringStatisticsTable {
            table_info,
            arg_database_name,
            arg_table_name,
        }))
    }
}

#[async_trait::async_trait]
impl Table for ClusteringStatisticsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        let args = vec![
            string_literal(self.arg_database_name.as_str()),
            string_literal(self.arg_table_name.as_str()),
        ];
        Some(TableArgs::new_positioned(args))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                ClusteringStatisticsSource::create(
                    ctx.clone(),
                    output,
                    self.arg_database_name.to_owned(),
                    self.arg_table_name.to_owned(),
                    plan.push_downs.as_ref().and_then(|x| x.limit),
                )
            },
            1,
        )?;

        Ok(())
    }
}

struct ClusteringStatisticsSource {
    finish: bool,
    ctx: Arc<dyn TableContext>,
    arg_database_name: String,
    arg_table_name: String,
    limit: Option<usize>,
}

impl ClusteringStatisticsSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        arg_database_name: String,
        arg_table_name: String,
        limit: Option<usize>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, ClusteringStatisticsSource {
            ctx,
            finish: false,
            arg_table_name,
            arg_database_name,
            limit,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ClusteringStatisticsSource {
    const NAME: &'static str = "fuse_block";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        let tenant_id = self.ctx.get_tenant();
        let tbl = self
            .ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                self.arg_database_name.as_str(),
                self.arg_table_name.as_str(),
            )
            .await?;
        let tbl = FuseTable::try_from_table(tbl.as_ref())?;

        let Some(cluster_key_id) = tbl.cluster_key_id() else {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table '{}.{}'",
                self.arg_database_name, self.arg_table_name,
            )));
        };

        Ok(Some(
            ClusteringStatistics::new(self.ctx.clone(), tbl, self.limit, cluster_key_id)
                .get_blocks()
                .await?,
        ))
    }
}

impl TableFunction for ClusteringStatisticsTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
