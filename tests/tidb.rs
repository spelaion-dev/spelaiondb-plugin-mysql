//! TiDB speaks MySQL's protocol and is reached through this driver as a MySQL connection
//! (SpelaionDB's SDB-170): the explorer's calls, a transaction followed from the server and
//! `KILL QUERY` must work on it as on MySQL.
//!
//! They need `SPELAION_TEST_TIDB=host:port` (`testing/compose.yaml`, user `root`, no
//! password). Without the variable they are skipped with a notice.

use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, ObjectKind,
    TransactionState,
};
use spelaion_driver_mysql::MysqlDriver;

async fn connect(database: &str) -> Option<Box<dyn Connection>> {
    let Ok(addr) = std::env::var("SPELAION_TEST_TIDB") else {
        eprintln!("SPELAION_TEST_TIDB not set: test skipped");
        return None;
    };
    let (host, port) = addr
        .rsplit_once(':')
        .expect("SPELAION_TEST_TIDB must be host:port");
    let params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("port"),
        user: "root".into(),
        database: database.into(),
        ..Default::default()
    };
    // The image has no health check of its own: TiDB answers a few seconds after it starts.
    let started = Instant::now();
    loop {
        match MysqlDriver::mysql().connect(&params).await {
            Ok(conn) => return Some(conn),
            Err(e) if started.elapsed() < Duration::from_secs(60) => {
                eprintln!("TiDB not ready yet: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("connecting to the test TiDB: {e}"),
        }
    }
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

#[tokio::test]
async fn tidb_browses_follows_transactions_and_cancels() {
    let Some(setup) = connect("").await else {
        return;
    };
    let db = format!("tidb_{}", std::process::id());
    run(setup.as_ref(), &format!("create database {db}"))
        .await
        .unwrap();
    let conn = connect(&db).await.unwrap();
    let conn = conn.as_ref();
    let checks = async {
        for sql in [
            "create table parent (id int primary key, name varchar(20))",
            "create table child (id int primary key, parent int, foreign key (parent) references parent (id))",
            "create view v as select id, name from parent",
        ] {
            run(conn, sql).await?;
        }
        let objects = conn.list_objects(&db).await?;
        let kind = |name: &str| objects.iter().find(|o| o.name == name).map(|o| o.kind);
        assert_eq!(kind("parent"), Some(ObjectKind::Table));
        assert_eq!(kind("v"), Some(ObjectKind::View));
        let child = conn.describe_table(&db, "child").await?;
        assert_eq!(child.primary_key, ["id"]);
        assert_eq!(child.foreign_keys.len(), 1);
        let keys = conn.list_referencing_keys(&db, "parent").await?;
        assert!(
            keys.iter()
                .any(|k| k.table == "child" && k.columns == ["parent"])
        );
        assert!(conn.relation_definition(&db, "v").await?.contains("VIEW"));
        conn.list_server_objects().await?;

        run(conn, "start transaction").await?;
        assert_eq!(conn.transaction_state().await?, TransactionState::Open);
        run(conn, "rollback").await?;
        assert_eq!(conn.transaction_state().await?, TransactionState::Idle);

        // KILL QUERY from a second connection stops a running statement.
        let cancel = conn.cancel_handle();
        let started = Instant::now();
        let (slept, cancelled) = tokio::join!(run(conn, "select sleep(30)"), async {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            cancel.cancel().await
        });
        cancelled?;
        assert!(started.elapsed() < Duration::from_secs(20));
        // TiDB answers a killed sleep with its result rather than an error: either way it ended.
        let _ = slept;
        Ok::<_, DriverError>(())
    };
    let result = checks.await;
    let _ = run(setup.as_ref(), &format!("drop database {db}")).await;
    result.unwrap();
}
