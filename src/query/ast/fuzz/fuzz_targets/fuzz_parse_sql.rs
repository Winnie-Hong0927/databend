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
// 使用 afl 库（American Fuzzy Lop）进行模糊测试
#[macro_use]
extern crate afl;

use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
// 导入 Backtrace 类型，用于跟踪解析过程中的错误。
use databend_common_ast::Backtrace;

fn main() {
    // 无限循环，反复执行模糊测试代码。
    loop {
        // afl 提供的宏，用于生成和运行模糊测试。它接受一个闭包，闭包的参数 text 是一个随机生成的字符串。
        fuzz!(|text: String| {
            let backtrace = Backtrace::new();
            // 将随机生成的 text 作为 SQL 语句进行词法分析。如果解析失败，会导致程序 panic，这在模糊测试中是可以接受的，因为它可以帮助发现问题。
            let tokens = tokenize_sql(&text).unwrap();
            // 将生成的 tokens 进行语法解析。如果解析失败，也会记录到 backtrace 中。
            let _ = parse_expr(&tokens, &backtrace);
        });
    }
}
