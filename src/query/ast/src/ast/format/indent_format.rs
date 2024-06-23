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

use std::fmt::Display;
use std::fmt::Write;

use super::FormatTreeNode;
use crate::Result;

static INDENT_SIZE: usize = 4;
// 一个树状结构的格式化实现，用于将树中的节点以缩进的方式打印出来
impl<T> FormatTreeNode<T>
where T: Display + Clone
{
    // 格式化树节点并返回格式化后的字符串
    pub fn format_indent(&self) -> Result<String> {
        let mut buf = String::new();
        self.format_indent_impl(0, &mut buf)?;
        Ok(buf)
    }
    // 递归地格式化树节点，并将结果写入到buf中
    fn format_indent_impl(&self, indent: usize, f: &mut String) -> Result<()> {
        writeln!(f, "{}{}", " ".repeat(indent), &self.payload).unwrap();
        for child in self.children.iter() {
            child.format_indent_impl(indent + INDENT_SIZE, f)?;
        }
        Ok(())
    }
}
