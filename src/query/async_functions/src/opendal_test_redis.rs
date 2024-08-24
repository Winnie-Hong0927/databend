use opendal::{services::Redis, Operator};

#[test]
fn opendal_test_redis() {
    let builder = Redis::default();
    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}