//! Cancelling a statement (SpelaionDB's SDB-222).
//!
//! MySQL and MariaDB cancel with `KILL QUERY <connection id>` sent over another connection,
//! and it only stops the statement that connection runs at that moment. A cancel pressed right
//! after Run, while the statement still waits in the actor's queue or has not reached the
//! server, used to land on an idle connection and do nothing, and the statement ran to its end.
//! Opening the second connection takes time too, so the window was wide.
//!
//! A cancel is remembered instead, by the rule of the core's `thread_cancel` (DuckDB, SQLite):
//! it applies to every statement sent before it, queued or running.
//! - A statement that has not started when its cancel arrives never starts: the actor checks
//!   before it runs one, and answers that it was cancelled.
//! - A statement that is running gets `KILL QUERY` again every [`KILL_AGAIN_EVERY`] until it
//!   ends, over one connection kept for that, so a `KILL` that arrived before the statement
//!   reached the server is followed by one it sees.
//! - A cancel never reaches a statement sent after it. A `KILL` is sent only while a cancelled
//!   statement is running, and while one is on its way the actor starts nothing (it waits in
//!   [`Canceller::begin`]); the server forgets a `KILL QUERY` when its next statement starts.
//! - A cancel with no statement in flight does nothing at all.
//!
//! MySQL ends `SLEEP()` with 1 instead of an error when it is killed: a statement that got a
//! `KILL` while it ran ends as cancelled even when the server says it succeeded
//! ([`Canceller::killed`]). Nothing is ever run again.

use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use spelaion_driver_api::{CancelHandle, Result};
use tokio::sync::Notify;

/// How often a running cancelled statement is killed again.
const KILL_AGAIN_EVERY: Duration = Duration::from_millis(50);

/// What a cancel names, read when the connection opened.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CancelTarget {
    /// The handshake's connection id, for `KILL QUERY`.
    Mysql(u32),
    /// GreptimeDB's `connection_id()`, the end of its process id.
    Greptime(u32),
}

#[derive(Default)]
struct State {
    /// Number of the last statement sent to the actor (numbers start at 1).
    submitted: u64,
    /// Number of the last statement the actor finished or skipped (they go in order).
    finished: u64,
    /// The statement the actor is running, if any.
    running: Option<u64>,
    /// Statements numbered up to this one are cancelled.
    cancelled_up_to: u64,
    /// A `KILL QUERY` is on its way: the actor starts nothing until it has landed.
    killing: bool,
    /// The last statement that got a `KILL` while it ran.
    killed: Option<u64>,
    /// A task is killing the running cancelled statement.
    interrupter: bool,
    /// The actor has ended: nothing will finish any more.
    closed: bool,
}

/// The cancel state of one connection, shared by its handle, its actor and the task a cancel
/// starts.
pub(crate) struct Canceller {
    state: Mutex<State>,
    changed: Notify,
    opts: Opts,
    timeout: Duration,
    target: CancelTarget,
}

/// What the actor does with a statement.
pub(crate) enum Begin {
    Run,
    /// Cancelled before it started: it is not sent.
    Skip,
}

impl Canceller {
    pub(crate) fn new(opts: Opts, timeout: Duration, target: CancelTarget) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            opts,
            timeout,
            target,
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A few counters, valid whatever a panicking holder left.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Numbers the next statement. The caller sends it to the actor in that order (it holds
    /// its sending lock), and calls [`Canceller::not_sent`] if it could not be sent.
    pub(crate) fn submit(&self) -> u64 {
        let mut state = self.lock();
        state.submitted += 1;
        state.submitted
    }

    /// Statement `id` never reached the actor (it has ended): nothing will run it.
    pub(crate) fn not_sent(&self, id: u64) {
        let mut state = self.lock();
        state.finished = state.finished.max(id);
        state.closed = true;
        drop(state);
        self.changed.notify_waiters();
    }

    /// The actor is about to send something to the server: statement `id`, or (with `None`)
    /// work of its own. It waits while a `KILL` is on its way, so the `KILL` cannot reach it,
    /// and a cancelled statement is skipped.
    pub(crate) async fn begin(&self, id: Option<u64>) -> Begin {
        loop {
            let mut notified = pin!(self.changed.notified());
            notified.as_mut().enable();
            {
                let mut state = self.lock();
                if !state.killing {
                    let Some(id) = id else { return Begin::Run };
                    if id <= state.cancelled_up_to {
                        state.finished = id;
                        drop(state);
                        self.changed.notify_waiters();
                        return Begin::Skip;
                    }
                    state.running = Some(id);
                    return Begin::Run;
                }
            }
            notified.await;
        }
    }

    /// Statement `id` got a `KILL` while it ran.
    pub(crate) fn killed(&self, id: u64) -> bool {
        self.lock().killed == Some(id)
    }

    /// The actor finished statement `id` (its result fully read or given up).
    pub(crate) fn end(&self, id: u64) {
        let mut state = self.lock();
        state.running = None;
        state.finished = id;
        drop(state);
        self.changed.notify_waiters();
    }

    /// The actor has ended.
    pub(crate) fn close(&self) {
        self.lock().closed = true;
        self.changed.notify_waiters();
    }

    /// Kills the running cancelled statement until every cancelled statement has finished.
    async fn kill_until_done(self: Arc<Self>) {
        let mut admin: Option<Conn> = None;
        loop {
            let mut notified = pin!(self.changed.notified());
            notified.as_mut().enable();
            let running = {
                let mut state = self.lock();
                if state.closed || state.finished >= state.cancelled_up_to {
                    state.interrupter = false;
                    break;
                }
                state.running.filter(|id| *id <= state.cancelled_up_to)
            };
            match (running, admin.as_mut()) {
                (Some(_), None) => {
                    // Connected first, without holding the actor back meanwhile.
                    admin = crate::open(self.opts.clone(), self.timeout).await.ok();
                    if admin.is_none() {
                        let _ = tokio::time::timeout(KILL_AGAIN_EVERY, notified).await;
                    }
                }
                (Some(_), Some(conn)) => {
                    // Checked again with the flag set: the statement may have ended meanwhile.
                    let target = {
                        let mut state = self.lock();
                        match state.running.filter(|id| *id <= state.cancelled_up_to) {
                            Some(id) => {
                                state.killing = true;
                                state.killed = Some(id);
                                true
                            }
                            None => false,
                        }
                    };
                    if target {
                        let sent = cancel_on(conn, self.target).await;
                        self.lock().killing = false;
                        self.changed.notify_waiters();
                        if sent.is_err() {
                            admin = None;
                        }
                        tokio::time::sleep(KILL_AGAIN_EVERY).await;
                    }
                }
                (None, _) => {
                    // Still queued, or between two cancelled statements.
                    let _ = tokio::time::timeout(KILL_AGAIN_EVERY, notified).await;
                }
            }
        }
        if let Some(conn) = admin {
            let _ = conn.disconnect().await;
        }
    }
}

/// The handle the core cancels with.
pub(crate) struct MysqlCancel(pub(crate) Arc<Canceller>);

#[async_trait]
impl CancelHandle for MysqlCancel {
    async fn cancel(&self) -> Result<()> {
        let canceller = &self.0;
        let mut state = canceller.lock();
        // Nothing sent is in flight: there is nothing to cancel, and a `KILL` now could only
        // reach whatever is sent next.
        if state.submitted == state.finished || state.closed {
            return Ok(());
        }
        state.cancelled_up_to = state.submitted;
        let start = !state.interrupter;
        state.interrupter = true;
        drop(state);
        canceller.changed.notify_waiters();
        if start {
            tokio::spawn(Arc::clone(canceller).kill_until_done());
        }
        Ok(())
    }
}

/// `KILL QUERY` of the connection. GreptimeDB accepts that and stops nothing: its queries are
/// killed by their process id, `<frontend>/<connection id>`, read from its process list. Only
/// a single match is killed: with several frontends, the same connection id on another one is
/// someone else's query.
async fn cancel_on(
    conn: &mut Conn,
    target: CancelTarget,
) -> std::result::Result<(), mysql_async::Error> {
    match target {
        CancelTarget::Mysql(id) => conn.query_drop(format!("KILL QUERY {id}")).await,
        CancelTarget::Greptime(id) => {
            let processes: Vec<String> = conn
                .exec(
                    "select id from information_schema.process_list where id like ?",
                    (format!("%/{id}"),),
                )
                .await?;
            if let [process] = processes.as_slice() {
                conn.query_drop(format!("KILL '{}'", process.replace('\'', "''")))
                    .await?;
            }
            Ok(())
        }
    }
}
