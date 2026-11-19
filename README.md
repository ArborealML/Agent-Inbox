# Agent-Inbox

Agent-Inbox batches notifications for existing Codex threads. Posting saves work immediately. The dispatcher waits for the recipient to declare a suitable work boundary before calling `codex queue`. A pause takes priority even when a routine digest awaits acknowledgment.

## Quick start

Install stable Rust and a Codex CLI that supports `codex queue --thread ... --message ...`. From this checkout run `cargo install --locked --path .`. Give producers and the dispatcher the same private **local** state directory. The example uses a POSIX shell. On PowerShell set `$env:AGENT_INBOX_DIR`, or pass `--dir PATH` to every command on either platform.

```sh
export AGENT_INBOX_DIR="$HOME/.agent-inbox"
mkdir -p -m 700 "$AGENT_INBOX_DIR"
agent-inbox watch
```

From a second shell using the same directory, substitute an existing thread UUID for THREAD.

```sh
agent-inbox post THREAD "Focused tests passed"
agent-inbox post THREAD "Review is ready"
agent-inbox state THREAD ready
agent-inbox status
```

New threads start busy. Once ready, messages accumulate for up to 30 seconds from the oldest pending item, and up to four messages travel in one digest. Old messages can go immediately on a ready boundary. Set `watch --batch-seconds N` to tune the window. When picking up work, mark the thread busy before acknowledging the batch ID printed in the digest. This keeps further routine notifications quiet until the next boundary.

```sh
agent-inbox state THREAD busy
agent-inbox ack THREAD 1
agent-inbox state THREAD waiting --key review-123
agent-inbox post THREAD --key review-123 "Review finished"
```

Waiting makes only exact-key results eligible, without the batching delay. Acknowledge the preceding routine digest before waiting for the next one. `ready` accepts ordinary notifications again. No session logs are scraped. Workers or their coordinator explicitly publish these states.

## Pause and recovery

`agent-inbox pause THREAD "Park after preserving your draft"` atomically parks routine delivery and queues an urgent message. The dispatcher prioritizes pauses at its next send decision. A send already in progress can finish. The pause is a queued instruction to the agent, not a process interrupt or confirmation of worker pickup. Acknowledge its ID as usual. To release the hold use `resume THREAD` followed by a fresh ready or waiting boundary.

The inbox uses bundled SQLite and one dispatcher lock per local directory. Rust's `notify` crate watches for filesystem changes. Startup and periodic scans recover missed events and the dispatcher keeps scanning if native watch setup fails. Producers can post while the dispatcher is offline. Messages, batch IDs, receipts and acknowledgments survive a restart. Sender failures retry with the same batch ID. A crash after Codex accepts a message but before the local receipt commits can duplicate that ID; acknowledge it once. After successful submission there is no automated redelivery of an unacknowledged digest. Check the worker thread if a receipt stays outstanding.

Use an owner-only directory on a local disk with platform-appropriate permissions. Anyone who can modify the inbox can send instructions to its configured threads. The database retains message bodies. The executable passed to `watch --sender PATH` must be trusted. It receives separate arguments without a shell. Agent-Inbox leaves session startup and resume to the operator.

## Development

`cargo fmt --check` and `cargo test --locked` exercise busy/ready transitions, batching, explicit wait keys, pause precedence, retry identity, restart, and delivery through a real dispatcher process to a fake sender. CI runs on Linux, macOS, and Windows. No live Codex session is required for these checks.
