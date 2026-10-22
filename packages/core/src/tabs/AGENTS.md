# Tabs

## What

Lets browser tabs share one persisted client replica. One tab leads: it owns the storage and the remote connection. Every tab syncs an in-memory replica with the leader.

## How to use

```typescript
// Automatic in a browser document when clientStorage has a name and a remote is set
const client = new TandemClient({ schema, remote, clientStorage })

// Elsewhere (a worker, tests), pass the locks and channels explicitly
import { InMemoryTabs } from "@tanishqkancharla/tandem-core/internal"
const tabs = new InMemoryTabs()
const client = new TandemClient({
	schema,
	remote,
	clientStorage,
	tabs: { environment: tabs.openTab() },
})
await client.close() // saves, then hands leadership to another tab
```

## How it works

1. **Election**: `TabSession` requests a lock named `tandem:<storage name>`. The holder calls `lead`, which builds the persisted `Database`, its `SyncEngine` with the real remote, and a `TabHub`. Other tabs stay queued for the lock and take over when it is released.
2. **Sessions**: `TandemClient` keeps an in-memory `Database` with its own client ID. Its `SyncEngine` uses `TabSession` as its remote, so pushes, pulls, and pokes travel over the channel. Requests wait until a leader announces itself and are resent when a new leader appears; the client then reconnects so pending mutations are pushed again.
3. **Push**: `TabHub` commits each forwarded mutation as its own leader mutation through `SyncEngine.commit(transaction, forwarded)`, which writes `["session", clientId]` with the tab's mutation ID in the same tuple transaction. Mutation IDs at or below that marker are skipped. A session without a marker starts at whatever it sends first. The hub answers once leader storage holds the writes and does not wait for the remote; the leader's outbox retries a failed remote push. A session's `commit` therefore resolves when the write is durable, and never stalls other tabs on the network.
4. **Pull**: the hub adds the session's scan window to the leader's subscriptions, so the leader pulls those records from the remote, and saves the window in the session's marker. A missing or stale cookie gets a reset patch with every record in the leader's replica. Otherwise the patch sets the scan window's records and removes every record sent earlier that left it. The hub flushes storage before answering, so a session never drops a mutation the leader could still lose.
5. **Handoff**: a new leader subscribes to every saved session window before it connects, so its first remote pull does not remove records the tabs still sync. Windows of sessions that do not pull within a minute belong to closed tabs and are dropped.
6. **Connection**: each tab asks the leader to connect or disconnect. The leader stays connected to the remote while any tab wants it, and tabs repeat their wish to a new leader. `autoConnect` sets a tab's initial wish.
7. **Pokes**: any commit that writes records pokes every session.
8. **Codecs**: records cross the channel by structured clone, so collection codecs encode values before sending and decode them on receipt.
9. **Timing**: a session's `SyncEngine` uses `TaskTimer`, so it reaches the leader on the next task instead of after `syncInterval`. The leader's `SyncEngine` and storage queue use `VisibilityAwareTimer`: their usual intervals while the page is visible, the next task while it is hidden, because browsers delay timers in hidden tabs but not messages.

## Limits

- A crashed follower keeps its queries in the leader's scan window, and its wish to stay connected, until the leader changes. Session markers that hold a mutation ID are never pruned.
- A follower's `commit` does not report remote push failures; the leader logs and retries them.
- A frozen leader tab (paused entirely, not just throttled) answers nothing until it resumes. Chrome does not freeze tabs that hold a Web Lock; other browsers may.
- Coordination needs a storage name and an environment. Without them a client owns its storage alone, so custom storage shared by tabs must set `name`.
- `clear()` in one tab clears the leader's replica for every tab. Other tabs keep and resend their unacknowledged mutations.
