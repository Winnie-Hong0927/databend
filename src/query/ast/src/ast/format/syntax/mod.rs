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

mod ddl;
mod dml;
mod expr;
mod query;

use ddl::*;
use dml::*;
use pretty::RcDoc;
use query::*;

use crate::ast::Statement;
use crate::ParseError;
use crate::Result;
//一个SQL解析器的一部分，它的作用是将SQL语句格式化，使其更易于阅读。

pub fn pretty_statement(stmt: Statement, max_width: usize) -> Result<String> {
    let pretty_stmt = match stmt {
        //根据语句的不同选择不同的方式将语句变得可读性更好
        //其中每一个解析器都使用了RcDoc应该是一个格式化工具
        //使用match模式匹配来判断一个语句中包含哪些元素，根据不同的元素拼接不同的SQL语句
        Statement::Query(query) => pretty_query(*query),
        Statement::Insert(insert_stmt) => pretty_insert(insert_stmt),
        Statement::Delete(delete_stmt) => pretty_delete(delete_stmt),
        Statement::CopyIntoTable(copy_stmt) => pretty_copy_into_table(copy_stmt),
        Statement::CopyIntoLocation(copy_stmt) => pretty_copy_into_location(copy_stmt),
        Statement::Update(update_stmt) => pretty_update(update_stmt),
        Statement::CreateTable(create_table_stmt) => pretty_create_table(create_table_stmt),
        Statement::AlterTable(alter_table_stmt) => pretty_alter_table(alter_table_stmt),
        Statement::CreateView(create_view_stmt) => pretty_create_view(create_view_stmt),
        Statement::AlterView(alter_view_stmt) => pretty_alter_view(alter_view_stmt),
        Statement::CreateStream(create_stream_stmt) => pretty_create_stream(create_stream_stmt),
        // Other SQL statements are relatively short and don't need extra format.
        // RcDoc 是一个文档构建器类型，用于构建格式化的文档。
        // pretty_statement 函数中使用 RcDoc 来构建格式化后的SQL语句。
        _ => RcDoc::text(stmt.to_string()),
    };

    let mut bs = Vec::new();
    pretty_stmt
        .render(max_width, &mut bs)
        .map_err(|err| ParseError(None, err.to_string()))?;
    String::from_utf8(bs).map_err(|err| ParseError(None, err.to_string()))
}

pub(crate) const NEST_FACTOR: isize = 4;
//辅助函数，用于在文档中插入逗号或点，以符合SQL语句的语法要求。
//用于在不同的上下文中插入逗号或点，并适当地处理换行和空格。
pub(crate) fn interweave_comma<'a, D>(docs: D) -> RcDoc<'a>
where D: Iterator<Item = RcDoc<'a>> {
    //parenthesized 函数用于将 RcDoc 包装在括号中，这在需要将子句分组时很有用。
    RcDoc::intersperse(docs, RcDoc::text(",").append(RcDoc::line()))
}

pub(crate) fn inline_comma<'a, D>(docs: D) -> RcDoc<'a>
where D: Iterator<Item = RcDoc<'a>> {
    RcDoc::intersperse(docs, RcDoc::text(",").append(RcDoc::space()))
}

pub(crate) fn inline_dot<'a, D>(docs: D) -> RcDoc<'a>
where D: Iterator<Item = RcDoc<'a>> {
    RcDoc::intersperse(docs, RcDoc::text("."))
}

pub(crate) fn parenthesized(doc: RcDoc<'_>) -> RcDoc<'_> {
    RcDoc::text("(")
        .append(RcDoc::line_())
        .append(doc)
        .nest(NEST_FACTOR)
        .append(RcDoc::line_())
        .append(RcDoc::text(")"))
        .group()
}
