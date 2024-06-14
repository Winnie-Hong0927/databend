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

//导入 criterion 库，并启用宏。
#[macro_use]
extern crate criterion;
// 从 criterion 库中导入所需的函数和类型。
use criterion::black_box;
use criterion::Criterion;
// 从 databend_common_ast 库中导入 SQL 解析相关的函数和类型。
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
// 定义一个基准测试函数，接受一个 Criterion 类型的可变引用作为参数。
fn bench(c: &mut Criterion) {
    // 创建一个基准测试组，命名为 bench_parser。
    let mut group = c.benchmark_group("bench_parser");
    // 设置基准测试组的样本大小为 10（即每个测试运行 10 次）。
    group.sample_size(10);
// 定义一个名为 large_statement 的基准测试函数。
    group.bench_function("large_statement", |b| {
        // 在每次迭代中运行以下代码。
        b.iter(|| {
            // 定义一个复杂的 SQL 语句。
            let case = r#"explain SELECT SUM(count) FROM (SELECT ((((((((((((true)and(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))or((('780820706')=('')))) IS NOT NULL AND ((((((((((true)AND(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))OR((('780820706')=(''))))) ::INT64)as count FROM t0) as res;"#;
            // 将 SQL 语句转换为 tokens（词法分析）
            let tokens = tokenize_sql(case).unwrap();
            // 解析 tokens，生成 SQL 抽象语法树（AST）。
            let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
            // 用 black_box 来防止编译器对未使用结果的优化。
            black_box(stmt);
        })
    });
    group.bench_function("large_query", |b| {
        b.iter(|| {
            let case = r#"SELECT SUM(count) FROM (SELECT ((((((((((((true)and(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))or((('780820706')=('')))) IS NOT NULL AND ((((((((((true)AND(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))OR((('780820706')=(''))))) ::INT64)as count FROM t0) as res;"#;
            let tokens = tokenize_sql(case).unwrap();
            let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
            black_box(stmt);
        })
    });
    group.bench_function("deep_query", |b| {
        b.iter(|| {
            let case = r#"SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers"#;
            let tokens = tokenize_sql(case).unwrap();
            let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
            black_box(stmt);
        })
    });
    group.bench_function("wide_expr", |b| {
        b.iter(|| {
            let case = r#"a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a"#;
            let tokens = tokenize_sql(case).unwrap();
            let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
            black_box(expr);
        })
    });
    group.bench_function("deep_expr", |b| {
        b.iter(|| {
            let case = r#"((((((((((((((((((((((((((((((1))))))))))))))))))))))))))))))"#;
            let tokens = tokenize_sql(case).unwrap();
            let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
            black_box(expr);
        })
    });
}
// 将定义的基准测试函数 bench 包装成一个基准测试组 benches。
criterion_group!(benches, bench);
// criterion_main!(benches);：定义程序的入口点，运行所有基准测试组。
criterion_main!(benches);
