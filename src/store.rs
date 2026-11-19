use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{path::Path, time::Duration};

pub fn open(dir: &Path) -> Result<Connection> {
    let db = Connection::open(dir.join("inbox.sqlite3"))?;
    db.busy_timeout(Duration::from_secs(5))?;
    db.execute_batch("PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS seats(seat TEXT PRIMARY KEY, state TEXT NOT NULL DEFAULT 'busy',
            wait_key TEXT, paused INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS batches(id INTEGER PRIMARY KEY, seat TEXT NOT NULL REFERENCES seats,
            urgent INTEGER NOT NULL, sent INTEGER NOT NULL DEFAULT 0, ack INTEGER NOT NULL DEFAULT 0,
            next_try INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY, seat TEXT NOT NULL REFERENCES seats,
            body TEXT NOT NULL, key TEXT, urgent INTEGER NOT NULL, created INTEGER NOT NULL,
            batch INTEGER REFERENCES batches);
        CREATE INDEX IF NOT EXISTS pending ON messages(seat,batch,urgent,id);
        CREATE INDEX IF NOT EXISTS active ON batches(seat,ack,urgent);")?;
    Ok(db)
}

pub fn register(db: &Connection, seat: &str) -> Result<()> {
    ensure!(!seat.trim().is_empty(), "empty thread");
    db.execute("INSERT OR IGNORE INTO seats(seat) VALUES(?)", [seat])?;
    Ok(())
}

pub fn post(
    db: &mut Connection,
    seat: &str,
    body: &str,
    key: Option<&str>,
    pause: bool,
    now: i64,
) -> Result<()> {
    ensure!(
        !body.trim().is_empty() && body.len() <= 2048,
        "message must contain 1–2048 bytes"
    );
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    register(&tx, seat)?;
    if pause {
        tx.execute(
            "UPDATE seats SET paused=1,state='busy',wait_key=NULL WHERE seat=?",
            [seat],
        )?;
    }
    tx.execute(
        "INSERT INTO messages(seat,body,key,urgent,created) VALUES(?,?,?,?,?)",
        params![seat, body, key, pause, now],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn boundary(db: &Connection, seat: &str, state: &str, key: Option<&str>) -> Result<()> {
    register(db, seat)?;
    db.execute(
        "UPDATE seats SET state=?,wait_key=? WHERE seat=?",
        params![state, key, seat],
    )?;
    Ok(())
}

pub struct Batch {
    pub id: i64,
    pub seat: String,
    pub urgent: bool,
    pub bodies: Vec<String>,
}

pub fn next(db: &mut Connection, now: i64, delay: i64) -> Result<Option<Batch>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // Recover reserved deliveries first. A new pause can bypass a pending routine batch.
    let mut found: Option<(i64, String, bool)> = tx
        .query_row(
            "SELECT b.id,b.seat,b.urgent FROM batches b JOIN seats s ON s.seat=b.seat
         WHERE b.sent=0 AND b.ack=0 AND b.next_try<=? AND (b.urgent OR
         (NOT s.paused AND (s.state='ready' OR (s.state='waiting' AND EXISTS
         (SELECT 1 FROM messages m WHERE m.batch=b.id AND m.key=s.wait_key)))))
         ORDER BY b.urgent DESC,b.id LIMIT 1",
            [now],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let candidate: Option<(String,bool)> = tx.query_row(
        "SELECT m.seat,m.urgent FROM messages m JOIN seats s ON s.seat=m.seat
         WHERE m.batch IS NULL AND (m.urgent OR (NOT s.paused AND
         ((s.state='ready' AND m.created<=?) OR (s.state='waiting' AND s.wait_key=m.key))
         AND NOT EXISTS(SELECT 1 FROM batches b WHERE b.seat=m.seat AND NOT b.urgent AND NOT b.ack)))
         ORDER BY m.urgent DESC,m.id LIMIT 1",[now-delay],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if let Some((seat, urgent)) = candidate {
        if found
            .as_ref()
            .is_none_or(|(_, _, prior_urgent)| urgent && !prior_urgent)
        {
            tx.execute(
                "INSERT INTO batches(seat,urgent) VALUES(?,?)",
                params![seat, urgent],
            )?;
            let id = tx.last_insert_rowid();
            // With bounded bodies and four items the digest fits Windows process argument limits.
            tx.execute(
                "UPDATE messages SET batch=? WHERE id IN (
                SELECT m.id FROM messages m JOIN seats s ON s.seat=m.seat
                WHERE m.seat=? AND m.batch IS NULL AND m.urgent=?
                AND (m.urgent OR s.state='ready' OR m.key=s.wait_key) ORDER BY m.id LIMIT 4)",
                params![id, seat, urgent],
            )?;
            found = Some((id, seat, urgent));
        }
    }
    let Some((id, seat, urgent)) = found else {
        return Ok(None);
    };
    let bodies = tx
        .prepare("SELECT body FROM messages WHERE batch=? ORDER BY id")?
        .query_map([id], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    // A failed or interrupted send retries the same ID. Only one daemon may send.
    tx.execute(
        "UPDATE batches SET next_try=? WHERE id=?",
        params![now + 5, id],
    )?;
    tx.commit()?;
    Ok(Some(Batch {
        id,
        seat,
        urgent,
        bodies,
    }))
}

pub fn ack(db: &Connection, seat: &str, id: i64) -> Result<()> {
    ensure!(
        db.execute(
            "UPDATE batches SET ack=1 WHERE seat=? AND id=?",
            params![seat, id]
        )? == 1,
        "unknown batch for thread"
    );
    Ok(())
}
