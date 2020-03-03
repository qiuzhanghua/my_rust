use r2d2;
use r2d2_mysql;

use r2d2_mysql::mysql::{Opts, OptsBuilder};
use r2d2_mysql::MysqlConnectionManager;
use std::sync::Arc;
use mysql::from_row;

#[derive(Debug, PartialEq, Eq)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}

fn main() {
    dotenv::dotenv().ok();
    // let db_url = "mysql://app:app@localhost:3306/app";
    let db_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let opts = Opts::from_url(&db_url).unwrap();
    let builder = OptsBuilder::from_opts(opts);
    let manager = MysqlConnectionManager::new(builder);
    let pool = Arc::new(r2d2::Pool::builder().max_size(4).build(manager).unwrap());
    let mut conn = pool.clone().get().unwrap();

    let x = conn.query(r#"select 4.1 x, version() v"#);
    // println!("{:?}", x.unwrap().last());
    for row in x.unwrap() {
        let (i, y) = from_row::<(f64, String)>(row.unwrap());
        println!("{:?}, {:?}", i, y);
    }
}
