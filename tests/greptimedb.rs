//! GreptimeDB speaks MySQL's protocol and is reached through this driver as a MySQL connection
//! (SpelaionDB's SDB-344): the explorer's calls must work on it although it has neither
//! `information_schema.triggers` nor `mysql.user`, a table's key must include its time index
//! (two rows may share the primary key at different times), and a long query must cancel.
//!
//! They need `SPELAION_TEST_GREPTIME=host:port` (`testing/compose.yaml`, no login). Without the
//! variable they are skipped with a notice.

use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType, ObjectKind,
};
use spelaion_driver_mysql::MysqlDriver;

async fn connect(database: &str) -> Option<Box<dyn Connection>> {
    let Ok(addr) = std::env::var("SPELAION_TEST_GREPTIME") else {
        eprintln!("SPELAION_TEST_GREPTIME not set: test skipped");
        return None;
    };
    let (host, port) = addr
        .rsplit_once(':')
        .expect("SPELAION_TEST_GREPTIME must be host:port");
    let params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("port"),
        database: database.into(),
        ..Default::default()
    };
    Some(
        MysqlDriver::mysql()
            .connect(&params)
            .await
            .expect("connecting to the test GreptimeDB"),
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

#[tokio::test]
async fn greptimedb_is_browsed_and_its_rows_are_keyed_by_their_time_index() {
    let Some(setup) = connect("public").await else {
        return;
    };
    let db = format!("greptime_{}", std::process::id());
    run(setup.as_ref(), &format!("create database {db}"))
        .await
        .unwrap();
    let conn = connect(&db).await.unwrap();
    let conn = conn.as_ref();
    let checks = async {
        run(
            conn,
            "create table cpu (host string, ts timestamp(3) time index, val double, \
             primary key (host))",
        )
        .await?;
        run(
            conn,
            "insert into cpu values ('a', 1000, 1.5), ('a', 2000, 2.5), ('b', 1000, 3.5)",
        )
        .await?;
        // The explorer's folders and the server's accounts: none, not an error.
        assert!(conn.list_schema_objects(&db).await?.is_empty());
        assert!(conn.list_server_objects().await?.is_empty());
        let objects = conn.list_objects(&db).await?;
        assert_eq!(
            objects.iter().find(|o| o.name == "cpu").map(|o| o.kind),
            Some(ObjectKind::Table)
        );
        // The key that pages and edits use: the primary key, then the time index.
        let table = conn.describe_table(&db, "cpu").await?;
        assert_eq!(table.primary_key, ["host", "ts"]);
        let names: Vec<_> = table.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["host", "ts", "val"]);
        assert!(conn.list_columns(&db).await?.len() >= 3);
        // Types as the core reads them.
        let events = run(conn, "select host, ts, val from cpu order by host, ts").await?;
        let ExecEvent::Columns(schema) = &events[0] else {
            panic!("columns first: {events:?}");
        };
        let logical: Vec<_> = schema.columns.iter().map(|c| c.logical).collect();
        assert_eq!(logical[0], LogicalType::Text);
        assert_eq!(logical[2], LogicalType::Float);
        let rows: usize = events
            .iter()
            .map(|e| match e {
                ExecEvent::Batch(b) => b.num_rows(),
                _ => 0,
            })
            .sum();
        assert_eq!(rows, 3);
        // A delete by the whole key reaches one row.
        run(conn, "delete from cpu where host = 'a' and ts = 1000").await?;
        let left = run(conn, "select count(*) from cpu where host = 'a'").await?;
        let count = left.iter().find_map(|e| match e {
            ExecEvent::Batch(b) => Some(b.num_rows()),
            _ => None,
        });
        assert_eq!(count, Some(1));
        Ok::<(), DriverError>(())
    }
    .await;
    let _ = run(setup.as_ref(), &format!("drop database {db}")).await;
    checks.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_long_greptimedb_query_is_cancelled() {
    let Some(conn) = connect("public").await else {
        return;
    };
    let conn: std::sync::Arc<dyn Connection> = conn.into();
    let cancel = conn.cancel_handle();
    // A `count(*)` answers only at the end, so `execute` itself waits on the server: run it
    // where the core does, and cancel from outside while it waits.
    let running = conn.clone();
    let reading = tokio::spawn(async move {
        running
            .execute(
                "select count(*) from generate_series(1, 100000000) a, generate_series(1, 1000) b",
                &ExecOptions::default(),
            )
            .await?
            .try_collect::<Vec<ExecEvent>>()
            .await
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!reading.is_finished(), "the query should still be running");
    let started = Instant::now();
    cancel.cancel().await.unwrap();
    let outcome: Result<Vec<ExecEvent>, DriverError> =
        tokio::time::timeout(Duration::from_secs(30), reading)
            .await
            .expect("the cancel was obeyed in time")
            .expect("the reading task ended");
    assert!(
        matches!(outcome, Err(DriverError::Cancelled)),
        "{outcome:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(20));
    // The session goes on.
    assert!(run(conn.as_ref(), "select 1").await.is_ok());
}
