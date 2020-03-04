use mysql::*;
use r2d2;
use r2d2::PooledConnection;
use r2d2_mysql;
use r2d2_mysql::mysql::{Opts, OptsBuilder};
use r2d2_mysql::MysqlConnectionManager;
use std::error::Error;
use std::result;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub struct Person {
    id: u64,
    name: String,
    email: String,
    enabled: Option<bool>,
}

fn main() -> result::Result<(), Box<dyn Error>> {
    dotenv::dotenv().ok();
    // let db_url = "mysql://app:app@localhost:3306/app";
    let db_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let opts = Opts::from_url(&db_url)?;
    let builder = OptsBuilder::from_opts(opts);
    let manager = MysqlConnectionManager::new(builder);
    let pool = Arc::new(r2d2::Pool::builder().max_size(4).build(manager)?);
    let mut conn = pool.get()?;
    let x = conn.query(r#"select 3.14 x, version() v"#)?;
    for row in x {
        let (i, y) = from_row::<(f64, String)>(row.unwrap());
        println!("{:?}, {:?}", i, y);
    }
    let names = query_databases(&mut conn)?;
    for name in names {
        println!("{:?}", name);
    }
    let names = query_tables(&mut conn)?;
    for name in names {
        println!("{:?}", name);
    }
    let col_infos = query_columns(&mut conn, "people")?;
    for ci in col_infos {
        println!("{:?}", ci);
    }

    let data = query_data(
        &mut conn,
        "select * from people where name = ? and enabled = ? limit ? offset ?",
        "Daniel",
        true,
        10,
        0,
    )?;
    for d in data {
        println!("{:?}", d);
    }
    Ok(())
}

pub fn query_databases(
    conn: &mut PooledConnection<MysqlConnectionManager>,
) -> result::Result<Vec<String>, Box<dyn Error>> {
    let qr = conn.query(r##"show databases;"##)?;
    let mut dbs = Vec::<String>::new();
    for row in qr {
        let name = from_row::<String>(row.unwrap());
        dbs.push(name);
    }
    Ok(dbs)
}

pub fn query_tables(
    conn: &mut PooledConnection<MysqlConnectionManager>,
) -> result::Result<Vec<String>, Box<dyn Error>> {
    let qr = conn.query(r##"show tables;"##)?;
    let mut tables = Vec::<String>::new();
    for row in qr {
        let name = from_row::<String>(row.unwrap());
        tables.push(name);
    }
    Ok(tables)
}

/// Fields, Type, Null, Key, Default, Extra
pub fn query_columns(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    table_name: &str,
) -> result::Result<Vec<(String, String, String, String, Option<String>, String)>, Box<dyn Error>> {
    if table_name.contains(' ') {
        // 小心SQL注入问题
        return Err(Box::<dyn Error>::from("table name error")); // sample for sql injection
    };
    let qr = conn.query(format!("describe {};", table_name))?;
    let mut cols = Vec::<(String, String, String, String, Option<String>, String)>::new();
    for row in qr {
        let col =
            from_row::<(String, String, String, String, Option<String>, String)>(row.unwrap());
        cols.push(col);
    }
    Ok(cols)
}

pub fn query_data(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    sql: &str,
    name: &str,
    enabled: bool,
    limit: u64,
    offset: u64,
) -> result::Result<Vec<(u64, String, String, Option<bool>)>, Box<dyn Error>> {
    let mut stmt = conn.prepare(sql)?;
    let qr = stmt.execute((name, enabled, limit, offset))?;
    let mut data = Vec::<(u64, String, String, Option<bool>)>::new();
    for row in qr {
        let r = from_row::<(u64, String, String, Option<bool>)>(row.unwrap());
        data.push(r);
    }
    Ok(data)
}

pub fn query_data_2(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    sql: &str,
    name: &str,
    enabled: bool,
    limit: u64,
    offset: u64,
) -> result::Result<Vec<Person>, Box<dyn Error>> {
    let x = conn
        .prep_exec(
            sql,
            params! {
            name,
            enabled,
            limit,
            offset,
            },
        )
        .map(|query_result| {
            query_result
                .map(|row_result| row_result.unwrap())
                .map(|row| {
                    let (id, name, email, enabled) =
                        from_row::<(u64, String, String, Option<bool>)>(row);
                    Person {
                        id,
                        name,
                        email,
                        enabled,
                    }
                })
                .collect()
        })
        .unwrap();
    Ok(x)
}

pub fn insert_data(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    name: &str,
    email: &str,
    enabled: bool,
) -> result::Result<u64, Box<dyn Error>> {
    let sql = "INSERT INTO people (name, email, enabled) VALUES (?, ?, ?)";
    let mut stmt = conn.prepare(sql)?;
    let qr = stmt.execute((name, email, enabled))?;
    Ok(qr.last_insert_id())
}

pub fn remove_data(
    conn: &mut PooledConnection<MysqlConnectionManager>,
    name: &str,
    email: &str,
    enabled: bool,
) -> result::Result<u64, Box<dyn Error>> {
    let sql = "DELETE FROM people WHERE name = ? and email = ? and enabled = ?";
    let mut tx = conn.start_transaction(true, None, None)?;
    let mut rows = 0;
    {
        let mut stmt = tx.prepare(sql)?;
        let qr = stmt.execute((name, email, enabled))?;
        rows = qr.affected_rows();
    }
    tx.commit();
    Ok(rows)
    // without transaction
    // let mut stmt = conn.prepare(sql)?;
    // let qr = stmt.execute((name, email, enabled))?;
    // Ok(qr.affected_rows())
}

#[cfg(test)]
mod tests {
    use super::*;
    use r2d2_mysql::mysql::{Opts, OptsBuilder};
    use r2d2_mysql::MysqlConnectionManager;
    use std::sync::Arc;

    #[test]
    fn test_insert() {
        dotenv::dotenv().ok();
        // let db_url = "mysql://app:app@localhost:3306/app";
        let db_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let opts = Opts::from_url(&db_url).unwrap();
        let builder = OptsBuilder::from_opts(opts);
        let manager = MysqlConnectionManager::new(builder);
        let pool = Arc::new(r2d2::Pool::builder().max_size(4).build(manager).unwrap());
        let mut conn = pool.get().unwrap();
        match insert_data(&mut conn, "Eason", "qiuyisheng@icloud.com", true) {
            Ok(id) => assert!(id > 0),
            Err(e) => assert!(false, e.to_string()),
        }
    }

    #[test]
    fn test_query_2() {
        dotenv::dotenv().ok();
        // let db_url = "mysql://app:app@localhost:3306/app";
        let db_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let opts = Opts::from_url(&db_url).unwrap();
        let builder = OptsBuilder::from_opts(opts);
        let manager = MysqlConnectionManager::new(builder);
        let pool = Arc::new(r2d2::Pool::builder().max_size(4).build(manager).unwrap());
        let mut conn = pool.get().unwrap();
        let v = query_data_2(
            &mut conn,
            "select * from people where name = :name and enabled = :enabled limit :limit offset :offset",
            "Daniel",
            true,
            10,
            0,
        );
        println!("{:?}", v);
        assert!(v.is_ok());
    }

    #[test]
    fn test_remove() {
        dotenv::dotenv().ok();
        // let db_url = "mysql://app:app@localhost:3306/app";
        let db_url = dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let opts = Opts::from_url(&db_url).unwrap();
        let builder = OptsBuilder::from_opts(opts);
        let manager = MysqlConnectionManager::new(builder);
        let pool = Arc::new(r2d2::Pool::builder().max_size(4).build(manager).unwrap());
        let mut conn = pool.get().unwrap();
        match remove_data(&mut conn, "Eason", "qiuyisheng@icloud.com", true) {
            Ok(count) => assert_eq!(count, 1),
            Err(e) => assert!(false, e.to_string()),
        }
    }
}
