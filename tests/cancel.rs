//! Cancelling a statement whatever moment the cancel arrives (SpelaionDB's SDB-222): before
//! the statement starts (still queued behind another one), at its start, and while it runs,
//! on MySQL and MariaDB. Each cancelled statement must end as cancelled within 10 seconds, and
//! a cancel must never reach a statement sent after it: one with nothing running touches
//! nothing, and the statement that follows a cancelled one runs to its end.
//!
//! They need `SPELAION_TEST_MYSQL=host:port` and/or `SPELAION_TEST_MARIADB=host:port` (user,
//! password and database: `spelaion`); a server without its variable is skipped.

use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, Secret,
};
use spelaion_driver_mysql::MysqlDriver;

/// A statement that runs for hours unless it is cancelled, and fails when it is.
const SLOW: &str = "select count(*) from information_schema.columns a, \
                    information_schema.columns b, information_schema.columns c";
/// The longest a cancelled statement may take to end.
const LIMIT: Duration = Duration::from_secs(10);

fn servers() -> Vec<(&'static str, MysqlDriver, String, u16)> {
    let mut list = Vec::new();
    for (label, var, driver) in [
        ("mysql", "SPELAION_TEST_MYSQL", MysqlDriver::mysql()),
        ("mariadb", "SPELAION_TEST_MARIADB", MysqlDriver::mariadb()),
    ] {
        match std::env::var(var) {
            Ok(addr) => {
                let (host, port) = addr.rsplit_once(':').expect("host:port");
                list.push((label, driver, host.into(), port.parse().expect("port")));
            }
            Err(_) => eprintln!("{var} not set: {label} skipped"),
        }
    }
    list
}

async fn connect(driver: &MysqlDriver, host: &str, port: u16) -> Arc<dyn Connection> {
    let params = ConnectParams {
        host: host.into(),
        port,
        user: "spelaion".into(),
        password: Some(Secret::new("spelaion")),
        database: "spelaion".into(),
        ..Default::default()
    };
    Arc::from(driver.connect(&params).await.expect("connecting"))
}

/// Runs `sql` to its end: every event, or the first error.
async fn run(conn: Arc<dyn Connection>, sql: &'static str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

/// The first value of `sql`'s result, as text.
async fn value(conn: &Arc<dyn Connection>, sql: &'static str) -> String {
    let events = tokio::time::timeout(LIMIT, run(conn.clone(), sql))
        .await
        .unwrap_or_else(|_| panic!("{sql}: did not end in time"))
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    events
        .iter()
        .find_map(|event| match event {
            ExecEvent::Batch(batch) if batch.num_rows() > 0 => {
                Some(arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no row"))
}

/// Waits for `task` to end, at most [`LIMIT`], and checks that it was cancelled.
async fn cancelled(
    label: &str,
    what: &str,
    task: tokio::task::JoinHandle<Result<Vec<ExecEvent>, DriverError>>,
) {
    let outcome = tokio::time::timeout(LIMIT, task)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{label}, {what}: still running {} s after the cancel",
                LIMIT.as_secs()
            )
        })
        .expect("the task");
    assert!(
        matches!(outcome, Err(DriverError::Cancelled)),
        "{label}, {what}: {outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_stops_a_statement_at_its_start_and_while_it_runs() {
    for (label, driver, host, port) in servers() {
        let conn = connect(&driver, &host, port).await;
        // At the start (the statement just sent, maybe not on the server yet) and while it
        // runs. Each time the same connection then runs something else normally.
        for delay in [1, 5, 20, 100, 1_000] {
            let task = tokio::spawn(run(conn.clone(), SLOW));
            tokio::time::sleep(Duration::from_millis(delay)).await;
            conn.cancel_handle().cancel().await.unwrap();
            cancelled(label, &format!("cancelled after {delay} ms"), task).await;
            assert_eq!(
                value(&conn, "select 42").await,
                "42",
                "{label}: after {delay} ms"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_statement_cancelled_before_it_starts_never_runs() {
    for (label, driver, host, port) in servers() {
        let conn = connect(&driver, &host, port).await;
        let marker = format!("sdb222_{label}_{}", std::process::id());
        // The second statement waits in the connection's queue behind the first: the cancel
        // reaches it before it starts, so it never runs (it would create the table).
        let first = tokio::spawn(run(conn.clone(), SLOW));
        tokio::time::sleep(Duration::from_millis(300)).await;
        let create: &'static str =
            Box::leak(format!("create table {marker} (id int)").into_boxed_str());
        let second = tokio::spawn(run(conn.clone(), create));
        tokio::time::sleep(Duration::from_millis(300)).await;
        conn.cancel_handle().cancel().await.unwrap();
        cancelled(label, "the running one", first).await;
        cancelled(label, "the queued one", second).await;
        let exists: &'static str = Box::leak(
            format!(
                "select count(*) from information_schema.tables where table_schema = database() and table_name = '{marker}'"
            )
            .into_boxed_str(),
        );
        assert_eq!(
            value(&conn, exists).await,
            "0",
            "{label}: the queued statement ran"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_killed_sleep_that_returns_a_value_ends_as_cancelled() {
    // MySQL ends a killed `SLEEP()` that is a query's only call with 1 and no error.
    for (label, driver, host, port) in servers() {
        let conn = connect(&driver, &host, port).await;
        let task = tokio::spawn(run(conn.clone(), "select sleep(30)"));
        tokio::time::sleep(Duration::from_millis(500)).await;
        conn.cancel_handle().cancel().await.unwrap();
        cancelled(label, "select sleep(30)", task).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_never_reaches_a_statement_sent_after_it() {
    for (label, driver, host, port) in servers() {
        let conn = connect(&driver, &host, port).await;
        // Nothing running: the cancel touches nothing, and the next statement runs its full
        // second (a `KILL QUERY` that reached it would end it at once, with 1).
        conn.cancel_handle().cancel().await.unwrap();
        let started = std::time::Instant::now();
        assert_eq!(value(&conn, "select sleep(1)").await, "0", "{label}");
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "{label}: the next statement was cut short"
        );
        // After a cancelled statement, and after a finished one cancelled too late.
        let task = tokio::spawn(run(conn.clone(), SLOW));
        tokio::time::sleep(Duration::from_millis(200)).await;
        conn.cancel_handle().cancel().await.unwrap();
        cancelled(label, "the slow statement", task).await;
        let started = std::time::Instant::now();
        assert_eq!(value(&conn, "select sleep(1)").await, "0", "{label}");
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "{label}: the statement after a cancelled one was cut short"
        );
        assert_eq!(value(&conn, "select 1").await, "1");
        conn.cancel_handle().cancel().await.unwrap();
        let started = std::time::Instant::now();
        assert_eq!(value(&conn, "select sleep(1)").await, "0", "{label}");
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "{label}: a late cancel reached the next statement"
        );
    }
}
