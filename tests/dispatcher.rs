use std::{
    path::Path,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};
struct Guard(Child);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn cli(dir: &Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_agent-inbox"));
    c.arg("--dir").arg(dir);
    c
}

#[test]
fn running_dispatcher_delivers_a_digest_without_shell_interpretation() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("state");
    let capture = temp.path().join("capture");
    let sender = temp
        .path()
        .join(format!("fake-sender{}", std::env::consts::EXE_SUFFIX));
    assert!(
        Command::new("rustc")
            .args(["tests/fixtures/fake_sender.rs", "-o"])
            .arg(&sender)
            .status()
            .unwrap()
            .success()
    );
    for args in [
        vec!["post", "test-worker", "literal $(echo hello)"],
        vec!["post", "test-worker", "second line"],
    ] {
        assert!(cli(&db).args(args).status().unwrap().success());
    }
    let _guard = Guard(
        cli(&db)
            .args(["watch", "--batch-seconds", "0", "--sender"])
            .arg(sender)
            .env("CAPTURE", &capture)
            .spawn()
            .unwrap(),
    );
    assert!(
        cli(&db)
            .args(["state", "test-worker", "ready"])
            .status()
            .unwrap()
            .success()
    );
    let observer = rusqlite::Connection::open(db.join("inbox.sqlite3")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if std::fs::read_to_string(&capture).unwrap_or_default()
            == "[Agent-Inbox digest #1]\nliteral $(echo hello)\n\nsecond line"
        {
            let sent: i64 = observer
                .query_row("SELECT sent FROM batches WHERE id=1", [], |r| r.get(0))
                .unwrap();
            if sent == 1 {
                break;
            }
        }
        assert!(Instant::now() < deadline, "digest never arrived");
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        cli(&db)
            .args(["ack", "test-worker", "1"])
            .status()
            .unwrap()
            .success()
    );
}
