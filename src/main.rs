mod store;

use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand, ValueEnum};
use fs2::FileExt;
use notify::{EventKind, RecursiveMode, Watcher};
use rusqlite::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Private local directory shared by all producers and one dispatcher.
    #[arg(long, env = "AGENT_INBOX_DIR")]
    dir: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}
#[derive(Clone, ValueEnum)]
enum State {
    Busy,
    Ready,
    Waiting,
}
#[derive(Subcommand)]
enum Cmd {
    /// Append a routine notification without contacting the recipient.
    Post {
        thread: String,
        message: String,
        #[arg(long)]
        key: Option<String>,
    },
    /// Declare a delivery boundary. Waiting requires an exact result key.
    State {
        thread: String,
        state: State,
        #[arg(long)]
        key: Option<String>,
    },
    /// Suspend routine delivery and queue an urgent instruction.
    Pause { thread: String, reason: String },
    /// Release the pause. Set a new ready/waiting boundary separately.
    Resume { thread: String },
    /// Acknowledge a received digest and allow the next eligible digest.
    Ack { thread: String, batch: i64 },
    /// Show queue state and outstanding batch IDs without printing message bodies.
    Status,
    /// Run the single dispatcher. Delivery uses codex queue without --resume.
    Watch {
        #[arg(long, default_value = "codex")]
        sender: PathBuf,
        #[arg(long, default_value_t=30, value_parser=clap::value_parser!(i64).range(0..=86400))]
        batch_seconds: i64,
    },
}

fn clock() -> Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64)
}

fn deliver(sender: &Path, b: &store::Batch) -> Result<()> {
    let kind = if b.urgent { "PAUSE" } else { "digest" };
    let msg = format!("[Agent-Inbox {kind} #{}]\n{}", b.id, b.bodies.join("\n\n"));
    let mut child = Command::new(sender)
        .arg("queue")
        .arg("--thread")
        .arg(&b.seat)
        .arg("--message")
        .arg(msg)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .context("start queue sender")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait()? {
            ensure!(status.success(), "sender exited with {status}");
            return Ok(());
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            child.wait()?;
            anyhow::bail!("sender timed out");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn watch(dir: &Path, db: &mut Connection, sender: &Path, delay: i64) -> Result<()> {
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("dispatcher.lock"))?;
    lock.try_lock_exclusive()
        .context("another dispatcher may own this inbox")?;
    let (wake, rx) = mpsc::sync_channel(1);
    let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if event.is_err()
            || event.is_ok_and(|e| {
                matches!(
                    e.kind,
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                )
            })
        {
            let _ = wake.try_send(());
        }
    })
    .and_then(|mut w| {
        w.watch(dir, RecursiveMode::NonRecursive)?;
        Ok(w)
    });
    if let Err(e) = &watcher {
        eprintln!("Native watch unavailable ({e}); using timed rescans")
    }
    loop {
        if let Some(batch) = store::next(db, clock()?, delay)? {
            match deliver(sender, &batch) {
                Ok(()) => {
                    db.execute("UPDATE batches SET sent=1 WHERE id=?", [batch.id])?;
                }
                Err(e) => {
                    eprintln!("batch {} delivery failed: {e}", batch.id);
                    db.execute(
                        "UPDATE batches SET next_try=? WHERE id=?",
                        rusqlite::params![clock()? + 5, batch.id],
                    )?;
                }
            }
        } else if watcher.is_ok() {
            let _ = rx.recv_timeout(Duration::from_secs(2));
        } else {
            std::thread::sleep(Duration::from_secs(2));
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    fs::create_dir_all(&cli.dir)?;
    let mut db = store::open(&cli.dir)?;
    match cli.cmd {
        Cmd::Post {
            thread,
            message,
            key,
        } => store::post(&mut db, &thread, &message, key.as_deref(), false, clock()?),
        Cmd::Pause { thread, reason } => {
            store::post(&mut db, &thread, &reason, None, true, clock()?)
        }
        Cmd::Resume { thread } => {
            ensure!(
                db.execute(
                    "UPDATE seats SET paused=0,state='busy',wait_key=NULL WHERE seat=?",
                    [thread]
                )? == 1,
                "unknown thread"
            );
            Ok(())
        }
        Cmd::State { thread, state, key } => {
            ensure!(
                matches!(state, State::Waiting) == key.as_ref().is_some_and(|s| !s.is_empty())
                    && key.as_ref().is_none_or(|s| !s.is_empty()),
                "supply a nonempty --key only for waiting"
            );
            store::boundary(
                &db,
                &thread,
                match state {
                    State::Busy => "busy",
                    State::Ready => "ready",
                    State::Waiting => "waiting",
                },
                key.as_deref(),
            )
        }
        Cmd::Ack { thread, batch } => store::ack(&db, &thread, batch),
        Cmd::Status => {
            let mut stmt=db.prepare("SELECT seat,state,paused,(SELECT count(*) FROM messages m WHERE m.seat=s.seat AND batch IS NULL),
                (SELECT group_concat(id) FROM batches b WHERE b.seat=s.seat AND NOT ack) FROM seats s ORDER BY seat")?;
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                println!(
                    "{} state={} paused={} pending={} outstanding={}",
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, bool>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, Option<String>>(4)?.as_deref().unwrap_or("-")
                );
            }
            Ok(())
        }
        Cmd::Watch {
            sender,
            batch_seconds,
        } => watch(&cli.dir, &mut db, &sender, batch_seconds),
    }
}
