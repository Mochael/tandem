# Sync Engine

## What

Synchronizes a client's local database with a remote server: pushes local mutations, pulls server patches for the client's subscriptions, and keeps optimistic writes visible until the server acknowledges them.

## How to use

```typescript
// Configured automatically by TandemClient
const remote: RemoteApi<Schema> = {
	async push({ clientId, mutations }) {
		// Apply the mutations in id order
		return { ok: true }
	},
	async pull({ clientId, cookie, scanWindow }) {
		// lastMutationId: the last of this client's mutations applied, on every pull
		return { cookie, patch, lastMutationId }
	},
	async connect({ clientId, poke }) {
		// Call poke() when the client should pull
		return async () => { /* unregister this client */ }
	},
}
```

## How it works

1. **Optimistic writes**: `TandemClient.commit` delegates to `SyncEngine.commit` when a remote is configured. `Database.commitMutation` commits application records, sparse bases, and an ordered outbox entry in one in-memory tuple transaction. A rejected transaction changes none of them and consumes no mutation ID. Local-only clients commit records directly without pending state.
2. **Push**: `SyncEngine` sends a copied snapshot of pending mutations to the remote. Neither success nor transport failure removes them; the initial commit promise still reports the push attempt.
3. **Pull**: `SyncEngine` fetches a patch for the scan window, which the client's subscriptions build. TandemServer keeps its latest generated view and opaque cookie per client. A missing or mismatched cookie returns a successful `patch.reset` snapshot instead of a diff. View changes, including subscription changes, receive a new cookie. `SyncEngine` retains the cookie after `Database.applyPull` commits the patch and acknowledgement.
4. **Rebase**: `["record"]` contains application-visible records, including optimistic changes. `["base", collection, ...id]` stores the latest known server value (or explicit absence) and `lastWrittenBy` only for records touched by pending mutations. `["outbox", mutationId]` stores ordered operations. A pull updates records and tracked bases, prunes acknowledged mutations, restores tracked records, replays remaining operations, then removes fully acknowledged bases before committing once. Record writes participate in normal transaction conflict detection even when the final value is unchanged. Snapshot resends and replay can therefore invalidate open drafts without a new application mutation. `SyncEngine` still owns identity, counter, and cookie until checkpoint persistence is implemented.
5. **Clear**: explicit `clear()` discards records, bases, and the outbox in one in-memory transaction. It starts a new client identity with a reset counter and cookie, preserves connected/disconnected state, and ignores old-session pull responses. Await clearing before starting new work. Already dispatched writes are not cancelled or undone on the server.
6. **Delivery**: each commit queues one push of the current pending snapshot. Failure rejects that commit promise but retains the mutation. A later commit includes older unacknowledged mutations; reconnect pulls first, then pushes anything still pending. There is no automatic retry, backoff, or confirming-pull loop. Without another commit or reconnect, a lost request stays pending locally.
7. **Pokes**: the remote tells a client when to pull. TaskQueue coalesces queued pulls; a poke during an in-flight pull queues a subsequent pull. Only a pull acknowledgement removes pending mutations, not a successful push response.

A confirmed write outside every subscription disappears on the next pull, because the server never sends that record back.

A reset clears application records and marks tracked bases absent before applying the snapshot, pruning acknowledged mutations, and replaying the remaining outbox. It does not discard unacknowledged mutations or change client identity. Storage still persists only the record cache, not bases, replica metadata, or the outbox; after restart, the initial reset removes cache records absent from the server. This provides convergence, not durable offline writes.

Expected remote failures are JSON-safe values tagged by `error`: `mutation-gap` (push only, with expected/received IDs), `invalid-request`, and `unavailable`. Remote callers must narrow those responses before reading pull data or treating a push as successful. Transport adapters return `unavailable` for expected network failures. SyncEngine retains pending writes and rejects the public operation with the protocol response as its cause; a failed batch may still have committed a prefix. Unexpected exceptions can still reject.
