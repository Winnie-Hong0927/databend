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

mod ast_format;
mod indent_format;
mod pretty_format;
mod syntax;

use std::fmt::Display;

pub use ast_format::format_statement;
pub use syntax::pretty_statement;
// 定义抽象语法树的格式化节点
#[derive(Clone)]
//T必须要实现了Display和Clone的trait，默认类型为String
pub struct FormatTreeNode<T: Display + Clone = String> {
    //用来存储节点的有效负载，即节点的具体内容，类型为 T。
    pub payload: T,
    //存储当前节点的子节点
    pub children: Vec<Self>,
}

impl<T> FormatTreeNode<T>
where T: Display + Clone
{
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            children: vec![],
        }
    }

    pub fn with_children(payload: T, children: Vec<Self>) -> Self {
        Self { payload, children }
    }
}
