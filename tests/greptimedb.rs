//! GreptimeDB speaks MySQL's protocol and is reached through this driver as a MySQL connection
//! (SpelaionDB's SDB-344): the explorer's calls must work on it although it has neither
//! `information_schema.triggers` nor `mysql.user`, a table's key must include its time index
//! (two rows may share the primary key at different times) and an append-only table must have
//! none (it keeps equal rows), and a long query must cancel without stopping another session's.
//!
//! They need `SPELAION_TEST_GREPTIME=host:port` (`testing/compose.yaml`, no login). Without the
//! variable they are skipped with a notice.

use std::sync::Arc;
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

/// The single number a `select count(*)` answers.
async fn count(conn: &dyn Connection, sql: &str) -> Result<i64, DriverError> {
    let events = run(conn, sql).await?;
    let batch = events
        .iter()
        .find_map(|e| match e {
            ExecEvent::Batch(b) if b.num_rows() == 1 => Some(b),
            _ => None,
        })
        .unwrap_or_else(|| panic!("one row from {sql}: {events:?}"));
    let column = batch.column(0);
    let text = arrow::util::display::array_value_to_string(column, 0)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    Ok(text
        .parse()
        .unwrap_or_else(|e| panic!("{sql}: {text}: {e}")))
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
        // That key is the row's identity: an insert with the same key and time replaces the
        // row, one with the same key at another time is another row.
        run(conn, "insert into cpu values ('a', 1000, 9.5)").await?;
        assert_eq!(count(conn, "select count(*) from cpu").await?, 3);
        assert_eq!(
            count(
                conn,
                "select count(*) from cpu where host = 'a' and ts = 1000 and val = 9.5"
            )
            .await?,
            1
        );
        // A delete by the whole key reaches one row.
        run(conn, "delete from cpu where host = 'a' and ts = 1000").await?;
        assert_eq!(
            count(conn, "select count(*) from cpu where host = 'a'").await?,
            1
        );

        // Without a primary key, the time index alone is the identity.
        run(
            conn,
            "create table bare (ts timestamp(3) time index, v double)",
        )
        .await?;
        run(conn, "insert into bare values (1000, 1), (1000, 2)").await?;
        assert_eq!(count(conn, "select count(*) from bare").await?, 1);
        assert_eq!(conn.describe_table(&db, "bare").await?.primary_key, ["ts"]);

        // An append-only table keeps equal rows: nothing identifies one, so it has no key.
        run(
            conn,
            "create table logs (host string, ts timestamp(3) time index, msg string, \
             primary key (host)) with (append_mode = 'true', ttl = '7d')",
        )
        .await?;
        run(
            conn,
            "insert into logs values ('a', 1000, 'x'), ('a', 1000, 'x')",
        )
        .await?;
        assert_eq!(count(conn, "select count(*) from logs").await?, 2);
        assert!(
            conn.describe_table(&db, "logs")
                .await?
                .primary_key
                .is_empty()
        );
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
    let conn: Arc<dyn Connection> = conn.into();
    // Another session runs a long query too: the cancel must stop this one, not that one
    // (every GreptimeDB process id ends with its own connection's number).
    let other: Arc<dyn Connection> = connect("public").await.unwrap().into();
    let reading = long_query(conn.clone());
    let other_reading = long_query(other.clone());
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!reading.is_finished(), "the query should still be running");
    let started = Instant::now();
    conn.cancel_handle().cancel().await.unwrap();
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
    assert!(
        !other_reading.is_finished(),
        "the other session's query was stopped too"
    );
    // The session goes on.
    assert!(run(conn.as_ref(), "select 1").await.is_ok());
    other.cancel_handle().cancel().await.unwrap();
    let other_outcome = tokio::time::timeout(Duration::from_secs(30), other_reading)
        .await
        .expect("the other cancel was obeyed in time")
        .expect("the other reading task ended");
    assert!(
        matches!(other_outcome, Err(DriverError::Cancelled)),
        "{other_outcome:?}"
    );
}

/// A `count(*)` answers only at the end, so `execute` itself waits on the server: run it where
/// the core does, and cancel from outside while it waits.
fn long_query(
    conn: Arc<dyn Connection>,
) -> tokio::task::JoinHandle<Result<Vec<ExecEvent>, DriverError>> {
    tokio::spawn(async move {
        conn.execute(
            "select count(*) from generate_series(1, 100000000) a, generate_series(1, 1000) b",
            &ExecOptions::default(),
        )
        .await?
        .try_collect::<Vec<ExecEvent>>()
        .await
    })
}
