use opendal::services::Mysql;

#[test]
fn opendal_mysql() {
    let mut build = Mysql::default()
        .root("/")
        .connection_string("mysql://root:123456@localhost/test")
        .table("emp")
        .key_field("name")
        .value_field("job");
    let op = Operator::new(build)?.finish();
    Ok(())
}
