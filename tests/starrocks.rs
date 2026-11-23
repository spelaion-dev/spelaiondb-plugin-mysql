//! StarRocks speaks MySQL's protocol and is reached through this driver as a MySQL connection
//! (SpelaionDB's SDB-310): the explorer's calls, a primary key table's key and `KILL QUERY` must
//! work on it as on MySQL, and what it cannot prepare (an import's INSERT, a data search's
//! `SET` of user variables) must be said in the capabilities, so the app refuses it up front.
//!
//! They need `SPELAION_TEST_STARROCKS=host:port` (the frontend's MySQL port, user `root`, no
//! password; `testing/docker/mysql-family` in SpelaionDB's repository). Without the variable they
//! are skipped with a notice.

use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, InsertTarget,
    ObjectKind, TextRows, bound_placeholder,
};
use spelaion_driver_mysql::MysqlDriver;

async fn connect(database: &str) -> Option<Box<dyn Connection>> {
    let Ok(addr) = std::env::var("SPELAION_TEST_STARROCKS") else {
        eprintln!("SPELAION_TEST_STARROCKS not set: test skipped");
        return None;
    };
    let (host, port) = addr
        .rsplit_once(':')
        .expect("SPELAION_TEST_STARROCKS must be host:port");
    let params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("port"),
        user: "root".into(),
        database: database.into(),
        ..Default::default()
    };
    Some(
        MysqlDriver::mysql()
            .connect(&params)
            .await
            .expect("connecting to the test StarRocks"),
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

#[tokio::test]
async fn starrocks_is_browsed_keyed_and_cancelled_and_refuses_what_it_cannot_prepare() {
    let Some(setup) = connect("").await else {
        return;
    };
    let db = format!("starrocks_{}", std::process::id());
    run(setup.as_ref(), &format!("create database {db}"))
        .await
        .unwrap();
    let conn = connect(&db).await.unwrap();
    let conn = conn.as_ref();
    let checks = async {
        for sql in [
            "create table parent (b int not null, a int not null, name varchar(20)) \
             primary key (b, a) distributed by hash(b) buckets 1 \
             properties ('replication_num' = '1')",
            "create table events (id int, amount double) duplicate key (id) \
             distributed by hash(id) buckets 1 properties ('replication_num' = '1')",
            "create table latest (id int not null, amount double) unique key (id) \
             distributed by hash(id) buckets 1 properties ('replication_num' = '1')",
            "create view v as select b, a, name from parent",
        ] {
            run(conn, sql).await?;
        }
        let schemas = conn.list_schemas().await?;
        assert!(schemas.iter().any(|s| s.name == db), "{schemas:?}");
        let objects = conn.list_objects(&db).await?;
        let kind = |name: &str| objects.iter().find(|o| o.name == name).map(|o| o.kind);
        assert_eq!(kind("parent"), Some(ObjectKind::Table));
        assert_eq!(kind("v"), Some(ObjectKind::View));
        // A primary key table's key, in key order; the other models keep no primary key.
        assert_eq!(
            conn.describe_table(&db, "parent").await?.primary_key,
            ["b", "a"]
        );
        for table in ["events", "latest"] {
            let info = conn.describe_table(&db, table).await?;
            assert!(
                info.primary_key.is_empty(),
                "{table}: {:?}",
                info.primary_key
            );
        }
        assert!(conn.relation_definition(&db, "v").await?.contains("VIEW"));
        conn.list_server_objects().await?;

        // What it cannot prepare is refused up front, and would indeed fail.
        let capabilities = conn.capabilities();
        assert!(!capabilities.insert_rows && !capabilities.bound_params);
        let target = InsertTarget {
            schema: db.clone(),
            table: "events".into(),
            columns: vec!["id".into(), "amount".into()],
        };
        let rows = TextRows::from_rows(2, &[vec![Some("1".into()), Some("1.5".into())]])?;
        assert!(conn.insert_rows(&target, rows).await.is_err());
        let p1 = bound_placeholder("mysql", 1);
        let search = async {
            conn.execute_bound(
                &format!("select id from events where id = {p1}"),
                vec!["1".into()],
                &ExecOptions::default(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await
        };
        assert!(search.await.is_err());

        // KILL QUERY from a second connection stops a running statement.
        let cancel = conn.cancel_handle();
        let started = Instant::now();
        let (slept, cancelled) = tokio::join!(run(conn, "select sleep(30)"), async {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            cancel.cancel().await
        });
        cancelled?;
        assert!(started.elapsed() < Duration::from_secs(20), "{slept:?}");
        Ok::<_, DriverError>(())
    };
    let result = checks.await;
    let _ = run(setup.as_ref(), &format!("drop database {db} force")).await;
    result.unwrap();
}
