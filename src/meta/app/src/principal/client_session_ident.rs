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

use crate::tenant_key::ident::TIdent;

/// Define the meta-service key for a user setting.
pub type ClientSessionIdent = TIdent<Resource>;

pub use kvapi_impl::Resource;

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::principal::client_session::ClientSession;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_session";
        const TYPE: &'static str = "ClientSession";
        const HAS_TENANT: bool = true;
        type ValueType = ClientSession;
    }

    impl kvapi::Value for ClientSession {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::client_session_ident::ClientSessionIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_setting_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let ident = ClientSessionIdent::new(tenant.clone(), "test");
        assert_eq!("__fd_session/tenant1/test", ident.to_string_key());

        let got = ClientSessionIdent::from_str_key(&ident.to_string_key()).unwrap();
        assert_eq!(ident, got);
    }
}