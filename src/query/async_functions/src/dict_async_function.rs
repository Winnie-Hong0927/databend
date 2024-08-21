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

use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_meta_app::schema::tenant_dictionary_ident::TenantDictionaryIdent;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::tenant::Tenant;
use educe::Educe;

use crate::AsyncFunctionCall;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct DictGetAsyncFunction {
    pub dict_name: String,
    pub db_name: String,
    pub catalog: String,
    pub tenant: Tenant,
}

impl DictGetAsyncFunction {
    pub async fn generate(
        &self,
        catalog: Arc<dyn Catalog>,
        async_func: &AsyncFunctionCall,
    ) -> Result<Scalar> {
        let tenant = &async_func.tenant;
        let db_name = &async_func.arguments[0];
        let db_id = catalog
            .get_database(tenant, db_name)
            .await?
            .get_db_info()
            .database_id
            .db_id;
        let dict_name = &async_func.arguments[1];
        let req = TenantDictionaryIdent::new(tenant, DictionaryIdentity::new(db_id, dict_name));
        let reply = catalog.get_dictionary(req).await?;
        match reply {
            Some(r) => return Ok(Scalar::Number(NumberScalar::UInt64(r.dictionary_id))),
            None => {
                return Err(ErrorCode::UnknownDictionary(format!(
                    "Unknown Dictionary {}",
                    dict_name,
                )));
            }
        }
    }
}