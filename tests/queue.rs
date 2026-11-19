#[path = "../src/store.rs"]
mod store;
use anyhow::Result;
use tempfile::tempdir;

#[test]
fn busy_then_batch_once_until_ack() -> Result<()> {
    let dir = tempdir()?;
    let mut db = store::open(dir.path())?;
    store::post(&mut db, "worker", "first", None, false, 100)?;
    assert!(store::next(&mut db, 200, 30)?.is_none());
    store::boundary(&db, "worker", "ready", None)?;
    store::post(&mut db, "worker", "second", None, false, 115)?;
    assert!(store::next(&mut db, 129, 30)?.is_none());
    let batch = store::next(&mut db, 130, 30)?.unwrap();
    assert_eq!(batch.bodies, ["first", "second"]);
    assert!(!batch.urgent);
    db.execute("UPDATE batches SET sent=1 WHERE id=?", [batch.id])?;
    store::post(&mut db, "worker", "third", None, false, 132)?;
    assert!(store::next(&mut db, 200, 30)?.is_none());
    assert!(store::ack(&db, "another", batch.id).is_err());
    store::ack(&db, "worker", batch.id)?;
    assert_eq!(store::next(&mut db, 201, 30)?.unwrap().bodies, ["third"]);
    Ok(())
}

#[test]
fn awaited_result_bypasses_batch_window_and_leaves_unrelated_items_pending() -> Result<()> {
    let dir = tempdir()?;
    let mut db = store::open(dir.path())?;
    store::boundary(&db, "seat", "waiting", Some("review-a"))?;
    store::post(&mut db, "seat", "other", Some("review-b"), false, 1)?;
    assert!(store::next(&mut db, 100, 30)?.is_none());
    store::post(&mut db, "seat", "accepted", Some("review-a"), false, 101)?;
    let b = store::next(&mut db, 101, 30)?.unwrap();
    assert_eq!(b.bodies, ["accepted"]);
    assert_eq!(b.seat, "seat");
    Ok(())
}

#[test]
fn restart_reuses_unsent_id_and_pause_bypasses_it() -> Result<()> {
    let dir = tempdir()?;
    let mut db = store::open(dir.path())?;
    store::boundary(&db, "seat", "ready", None)?;
    store::post(&mut db, "seat", "old", None, false, 0)?;
    let first = store::next(&mut db, 0, 0)?.unwrap().id;
    drop(db);
    let mut db = store::open(dir.path())?;
    store::post(&mut db, "seat", "stop now", None, true, 1)?;
    let stop = store::next(&mut db, 1, 30)?.unwrap();
    assert!(stop.urgent);
    assert_eq!(stop.bodies, ["stop now"]);
    store::ack(&db, "seat", stop.id)?;
    store::boundary(&db, "seat", "ready", None)?;
    assert!(store::next(&mut db, 100, 0)?.is_none());
    db.execute("UPDATE seats SET paused=0", [])?;
    assert_eq!(store::next(&mut db, 101, 0)?.unwrap().id, first);
    Ok(())
}
