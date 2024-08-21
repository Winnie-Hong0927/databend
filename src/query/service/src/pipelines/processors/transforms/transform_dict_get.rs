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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Value;
use databend_common_meta_app::schema::tenant_dictionary_ident::TenantDictionaryIdent;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_storages_fuse::TableContext;

use crate::sessions::QueryContext;

pub struct TransformDictGet {
    ctx: Arc<QueryContext>,
    dict_name: String,
    return_type: DataType,
}

impl TransformDictGet {
    pub fn new(ctx: Arc<QueryContext>, dict_name: &str, return_type: &DataType) -> Self {
        Self {
            ctx,
            dict_name: dict_name.to_owned(),
            return_type: return_type.clone(),
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for TransformDictGet {
    const NAME: &'static str = "DictSource";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        if data_block.is_empty() {
            return Ok(data_block);
        }
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_default_catalog()?;
        // The first entry of data_block stores db_name, and the second entry stores dict_name.
        let db_name = data_block.columns()[0].value.to_string();
        let db_id = catalog
            .get_database(&tenant, db_name.as_str())
            .await?
            .get_db_info()
            .database_id
            .db_id;
        let req = TenantDictionaryIdent::new(
            tenant.clone(),
            DictionaryIdentity::new(db_id, self.dict_name.clone()),
        );
        let reply = catalog.get_dictionary(req).await?;
        match reply {
            Some(r) => {
                let vec = vec![r.dictionary_id, r.dictionary_meta_seq];
                let value = UInt64Type::from_data(vec);
                let entry = BlockEntry {
                    data_type: self.return_type.clone(),
                    value: Value::Column(value),
                };
                data_block.add_column(entry);
                return Ok(data_block);
            }
            None => {
                return Err(ErrorCode::UnknownDictionary(format!(
                    "Unknown Dictionary {}",
                    self.dict_name.clone(),
                )));
            }
        }
    }
}
