# Client storage tuples

Phases 1 and 2 are merged. Phases 3 and 4 are implemented and verified on `client-durable-checkpoints`. Complete checkpoints reach storage before delivery, and startup restores the saved session before reconnecting. The full-confirmed-replica and `["visible"]` proposal is replaced.

## Problem overview

`TandemClientStorageApi` previously accepted only application-record tuples. IndexedDB selected a collection codec from the second key element without distinguishing other namespaces. That worked for records, but not for replica metadata or an outbox containing nested record values.

The old write path assumed every value was a collection record:

```callstack
 TandemClientIndexedDbStorage.commit [[indexeddb:old:145-157]]
 ├── open an IndexedDB write transaction
 ├── select the collection codec from key[1]
 ├── encode and write each value
 └── await transaction completion
```

An outbox `set` must restore the same runtime value as an ordinary record. For example, an application codec that rebuilds an `EventStart` instance must also run on that value when it is nested in a pending mutation.

## Solution overview

Keep the tuple-storage operations and the application-record namespace. Local changes continue to write directly to `["record"]`. Store pending mutations and their sparse server base in sibling tuple namespaces, instead of a separate array and map. No full confirmed-record copy, `["visible"]` projection, or new storage method is needed.

```ts
// Current tuple schema. Sync reads replica metadata from the database.
type ReplicaTuples =
  | { key: ["record", collection, ...id]; value: ApplicationRecord }
  | {
      key: ["replica"];
      value: { clientId: ClientId; lastIssuedMutationId: MutationId; cookie?: Cookie };
    }
  | { key: ["outbox", MutationId]; value: MutationOp[] }
  | {
      key: ["base", collection, ...id];
      value: {
        server: { type: "set"; value: ApplicationRecord } | { type: "remove" };
        lastWrittenBy: MutationId;
      };
    };
```

The outbox key owns the mutation ID; its value holds operations in their original order. A base entry exists only for a record touched by an unacknowledged mutation. It preserves the latest known server value, including explicit absence, and the newest pending mutation that touches the record. `lastIssuedMutationId` and the cookie remain distinct: one prevents mutation-ID reuse, while the other identifies a pulled view.

## Goals

- Represent records, replica metadata, and pending operations in one typed tuple schema.
- Round-trip custom application values inside records, outbox operations, and base entries.
- Keep application queries and the existing reconciliation algorithm while moving pending state into tuples.
- Persist complete checkpoints and restore them before reconnecting, so offline writes survive restart.

## Non-goals

Phase 1 alone does not provide durable offline writes. Later phases do not introduce confirmed-only record storage, a separate visible namespace, automatic network retries, backoff, or a confirming-pull loop. Storage continues to accept tuple scans and commits; atomic checkpointing, not a new storage API, provides durability.

## Implementation

### ✅ Phase 1: Define and encode durable replica tuples

**Today**

Before this change, `SchemaToTupleSchema<Schema>` described the entire client-storage boundary, and only allowed application records. The IndexedDB adapter treated every tuple as one of those records.

**Proposed**

Add `TandemClientTuple<Schema>` as the storage boundary's union, retaining `SchemaToTupleSchema<Schema>` for application records. Teach the existing adapter to encode and decode the new values. This phase is implemented and verified.

The contract grows without adding operations. `TandemClientStorage` forwards the same batches to its adapter; the public export and DST storage implementation use the widened tuple type as well.

```callstack
 TandemClientStorage.commit [[storage:new:83-90]]
-└── accept only application-record tuples [[storage:old:5-10]]
+└── accept TandemClientTuple batches # store all namespaces through one commit [[storage:new:8-28]]
```

`createTupleCodec` composes the collection codecs into one codec for complete `TandemClientTuple` values and encoded IndexedDB entries. The adapter stores that codec and calls its `encode` and `decode` methods. Metadata and entire mutation envelopes are not passed to collection codecs.

```callstack
+createTupleCodec [[indexeddb:new:63-117]]
+├── transform record values with their collection codec [[indexeddb:new:71-74]]
+├── preserve replica metadata unchanged [[indexeddb:new:75-76]]
+└── map outbox operations in order [[indexeddb:new:77-91]]
+   ├── transform each set value with its collection codec [[indexeddb:new:80-85]]
+   └── preserve remove operations unchanged [[indexeddb:new:86-87]]
```

Encoding now finishes before opening the write transaction. If a codec rejects an outbox value, no metadata or earlier record in the batch has been written. This is a codec-failure guarantee, not a replacement for the later persistence-ordering work.

```callstack
 TandemClientIndexedDbStorage.commit [[indexeddb:new:195-208]]
-├── open the transaction before encoding [[indexeddb:old:146-153]]
+├── tupleCodec.encode # encode complete tuples before writing [[indexeddb:new:196-198]]
 ├── open one write transaction [[indexeddb:new:199-200]]
 ├── write encoded sets and removals [[indexeddb:new:201-206]]
 └── await transaction completion [[indexeddb:new:207]]
```

Reading takes the inverse path. `scan()` decodes through the composed codec. `Database.loadFromStorage()` scans only the record prefix, using tuple-database's bounds normalization rather than loading and filtering unrelated tuples. Metadata and outbox hydration are intentionally not wired into `SyncEngine` yet.

```callstack
 Database.loadFromStorage [[database:new:122-126]]
+├── normalizeTupleBounds # restrict storage reads to the record prefix [[database:new:123-125]]
 ├── TandemClientStorage.scan [[storage:new:93-96]]
 │  └── TandemClientIndexedDbStorage.scan [[indexeddb:new:182-192]]
+│     └── tupleCodec.decode # restore complete tuples [[indexeddb:new:185-187]]
-└── load every stored tuple into the visible database [[database:old:121-123]]
+└── load scanned records into the visible database [[database:new:126]]
```

Verification exercises the adapter through its public API: close and reopen a mixed batch, restore class instances inside pending operations, preserve operation order, remove an outbox entry without deleting metadata, and reject an encoding failure without writing a prefix. Type checks reject outbox operations for the wrong application collection.

The implementation passed `pnpm build`, `pnpm test` (146 tests), `pnpm type-check`, `pnpm lint`, formatting checks, and `git diff --check`. These checks verify this schema foundation, not crash-safe outbox delivery.

### ✅ Phase 2: Move PendingWrites state into database tuples

**Today**

Before this phase, `SyncEngine` owns `PendingWrites`. That class stores an ordered mutation array and a sparse base map whose entries contain a server operation and `lastWrittenBy`. Application records already contain optimistic changes.

**Proposed**

Replace the mutation array with `["outbox", mutationId]` tuples and the base map with `["base", collection, ...id]` tuples. Preserve the reconciliation algorithm and write optimistic changes directly into `["record"]`. Stage record changes and pending-state changes in the same root transaction. This phase is implemented; identity, cookie, and the counter remain in `SyncEngine` until phase 3.

`Database.commitMutation` preserves an existing base's server value or captures the committed record when first tracking it. It reads the current database outside the draft so it neither captures an optimistic edit nor adds bookkeeping keys to the user's read set. Every touched base advances `lastWrittenBy`. The outbox stores a copy of the operation list in its original order.

```callstack
 SyncEngine.commit [[sync2:new:267-278]]
-├── PendingWrites.commitAndTrack [[sync2:old:276-279]]
+├── Database.commitMutation [[database2:new:238-254]]
+│  ├── capture or retain sparse server bases and advance lastWrittenBy [[database2:new:240-251]]
+│  ├── stage the ordered outbox entry [[database2:new:253]]
+│  └── TupleRootTransaction.commit # publish records and pending state together [[database2:new:254]]
 └── enqueue the push
```

The counter advances only after a successful commit. ID allocation also considers the already-published outbox: a synchronous subscription callback can commit before the outer call returns. This prevents duplicate IDs and lost writes. A rejected draft publishes no records, bases, or mutation and consumes no ID.

`Database` and `Transaction` expose typed record, base, and outbox subspaces. Their getters only call `subspace` and assert its type, with comments explaining why tuple-database cannot reduce the generic namespace filter. Transaction views share the original root transaction.

```callstack
 Database.applyPull [[database2:new:257-260]]
+├── Transaction.records # select the record namespace on the same transaction [[transaction2:new:175-180]]
+├── Transaction.bases # select the base namespace on the same transaction [[transaction2:new:182-187]]
+└── Transaction.outbox # select the outbox namespace on the same transaction [[transaction2:new:189-194]]
```

Record and base views accept the schema's record union for cross-collection reconciliation; public collection methods and the durable tuple schema retain collection-specific record and ID types. No pending-mutation wrapper remains. Storage removes and restores the record prefix using tuple-database's helpers, with assertions at that key-conversion boundary because those helpers erase tuple types.

Push reads the outbox in mutation-ID order. Neither push success nor failure removes an entry; only a pull acknowledgement does. `TandemClientBase` describes sparse server values. The tuple codec now transforms nested base set values, just as it transforms nested outbox set values. A base removal marker passes through unchanged. The storage API itself stays the same.

```callstack
 SyncEngine.push [[packages/core/src/sync/SyncEngine.ts#SyncEngine.push]]
-├── PendingWrites.snapshot [[sync2:old:294]]
+├── Database.outbox # select the typed subspace [[database2:new:121-125]]
+├── scan outbox tuples and build the push payload [[sync2:new:284-287]]
 └── RemoteApi.push
```

A pull updates application records and any tracked server bases, removes acknowledged outbox entries, restores tracked records to their server values, and replays the remaining operations. Base entries whose newest writer was acknowledged are removed only after restoration. Otherwise an acknowledged optimistic record absent from the server's scan window could remain visible.

```callstack
 SyncEngine.pull [[packages/core/src/sync/SyncEngine.ts#SyncEngine.pull]]
-├── PendingWrites.applyPull [[sync2:old:265-268]]
+└── Database.applyPull [[database2:new:257-294]]
+   ├── Database.applyServerOp # apply patches and update tracked bases [[database2:new:297-320]]
+   ├── remove outbox IDs at or below lastMutationId [[database2:new:271-272]]
+   ├── restore tracked records from their updated bases [[database2:new:273-285]]
+   ├── applyOp # replay remaining operations in order [[database2:new:286-289]]
+   ├── remove fully acknowledged bases after restoration [[database2:new:290-293]]
+   └── TupleRootTransaction.commit # publish every staged write once [[database2:new:294]]
```

Pull writes follow ordinary write-based conflict detection. They are not filtered by structural equality or by whether a removed record already exists. An intervening write can invalidate an open draft even when replay restores the same visible record. Snapshot resends and replay can also cause conservative conflicts without a new application mutation. Field-level patches and distinguishing new mutations from synchronization bookkeeping remain deferred.

On reset, clear only application records and mark every tracked base as absent before applying the snapshot. Preserve the outbox until acknowledgement. Replay one operation at a time: tuple-database's batch `write()` applies removes before sets and would otherwise reorder a mutation that sets and then removes the same record.

Explicit `clear()` discards records, bases, and the outbox and retains the existing fresh-session behavior. Local-only clients keep their direct record commits and do not create pending state. The in-memory tuple database uses `TandemClientTuple<AnySchema>` because tuple-database cannot narrow generic mapped keys; application methods and the storage boundary retain their schema-specific types. There is one database and no second record projection.

Initial phase 2 verification passed `pnpm build`, `pnpm type-check`, `pnpm test` (149 tests), `pnpm lint`, formatting checks, and `git diff --check`. Consumer regressions cover nested subscription commits and rejected drafts leaving no ghost mutation or ID gap. The original draft-survival expectation is replaced by write-based conflict detection: a pull rejects the stale draft without publishing partial writes, and a fresh transaction delivers the pending edits. That revised regression fails with equality filtering enabled and passes after removing it. The adapter test also restores codec-backed base values and explicit absence after reopening IndexedDB.

Two DST sweeps passed 32 runs each, with 500 steps per run and zero violations, nondeterminism, or errors. Seeds 2026101500–2026101531 use fault rate 0.15 without crashes; seeds 2026101600–2026101631 use fault rate 0.1 and crash rate 0.05. These sweeps verify convergence after draining calls. This phase does not persist or restore the outbox and therefore does not establish durable offline delivery.

After the subspace-wrapper refactor, the same build, type, lint, and 149-test checks passed again. A further sweep passed seeds 2026101700–2026101715 with 500 steps, fault rate 0.1, and crash rate 0.05, with zero violations, nondeterminism, or errors.

After removing equality filtering, the build, type checks, lint, and all 149 tests passed. The updated conflict regression failed before the fix and passed afterward. Two additional 16-run DST sweeps passed with 500 steps per run: seeds 2026101800–2026101815 at fault rate 0.15 without crashes, and seeds 2026101900–2026101915 at fault rate 0.1 and crash rate 0.05. Both reported zero violations, nondeterminism, or errors. Temporary public-client probes also verified changed Map values reach clients through ordinary and reset pulls; those probes were removed after validation.

### ✅ Phase 3: Persist complete checkpoints before delivery

**Today**

After phase 2, records and pending-write state are transactional in memory, but storage still saves only the application-record cache. `SyncEngine` still keeps identity, cookie, and the counter in fields. The existing storage queue does not provide an ordered durability barrier.

**Proposed**

Move identity, `lastIssuedMutationId`, and cookie into `["replica"]` and include their changes in the same root transactions as records, bases, and outbox entries. Persist every namespace through `TandemClientStorageApi.commit`. Optimistic records are safe to persist when their base and outbox are part of the same atomic checkpoint; there is no need to persist only confirmed records. This phase is implemented and verified locally.

`Database` now buffers root tuple writes. `ThrottleQueue` serializes batches: a commit arriving during an active save belongs to a later batch. Failed changes merge before newer queued changes. `flushClientStorage()` waits for queued and active writes; it releases the existing timer delay rather than executing a duplicate task. Explicit clear supersedes queued old-session writes and runs after any active save, so that save cannot resurrect discarded state.

A local write stays immediately visible, but its network delivery waits for its checkpoint. The public commit promise retains its existing push-attempt result, with storage failure rejecting before any push. A failed checkpoint leaves the mutation pending and unsent; an explicit flush or later work can retry it. There is no automatic backoff or retry loop.

`SyncEngine.push` also flushes storage after copying the pending mutation snapshot. That snapshot can include newer mutations whose storage batches are still in flight. It captures the client identity and rechecks the session after waiting, so an intervening clear cannot send old mutations under a new identity.

```callstack
 SyncEngine.commit [[packages/core/src/sync/SyncEngine.ts#SyncEngine.commit]]
+├── Database.commitMutation [[database3:new:285-286]] [[database3:new:301]] # publish records, bases, outbox, and replica metadata together
+├── Database.persistClientStorage [[sync3:new:266-273]] # await the ordered checkpoint
 └── enqueue the push # do not send a mutation before it is durable
```

A pull transaction now includes its cookie alongside record changes, bases, and acknowledgement pruning. The storage adapter receives those changes in one atomic batch. If storage fails, disk retains the previous checkpoint and the complete failed batch remains buffered.

```tsx
// Pseudocode: local visibility is immediate; delivery waits for durability.
db.commitMutation(tx) // records + bases + outbox + incremented replica counter
await db.persistClientStorage()
await syncQueue.enqueue("push")

// A pull checkpoints the server view and acknowledgement together.
db.applyPull(response) // records + bases + pruned outbox + replica cookie
await db.persistClientStorage()
```

The later push barrier is separate from the commit barrier because a coalesced push can include more than one commit:

```callstack
 SyncEngine.push [[packages/core/src/sync/SyncEngine.ts#SyncEngine.push]]
 ├── copy the outbox and client identity
+├── Database.flushClientStorage [[sync3:new:284-287]] # cover every copied mutation
+├── recheck client identity and connection [[sync3:new:287]]
 └── remote.push # deliver the durable snapshot
```

Build, type checks, lint, and all 155 tests pass. New consumer tests exercise real IndexedDB checkpoints before push, atomic pull checkpoint retries, failed-batch merging with newer writes, a newer mutation captured by an older push, flush failure propagation, and clear during an active save. The queue regression verifies flush cannot duplicate or overtake an active batch. The checkpoint-before-send and queue regressions both fail on unchanged main.

DST fixtures now include storage handoffs before network delivery; the lost-request check explicitly drops the outbound request rather than relying on a seed's former scheduling. Two 16-run sweeps of 500 steps passed: seeds 2026102000–2026102015 at fault rate 0.15, and seeds 2026102100–2026102115 at fault rate 0.1 and crash rate 0.05. Both reported zero violations, nondeterminism, and errors. These sweeps establish convergence, not phase-4 preservation of offline writes after restart.

### ✅ Phase 4: Restore the checkpoint before connecting and resume delivery

**Today**

After phase 3, storage contains complete checkpoints, but `Database.loadFromStorage` still scans only `["record"]` and construction creates a fresh identity. Connection waits for the initial checkpoint save, which waits for record loading. It does not hydrate saved replica metadata, bases, or the outbox, and synchronized writes are not gated on hydration. Restart cannot yet resume the saved session.

**Proposed**

Load records, bases, outbox, and replica metadata before accepting transactions or connecting. Restore the saved identity, counter, and cookie rather than overwriting them during `SyncEngine` construction. For a fresh store, initialize and persist replica metadata before its first connection. Resolve `TandemClient.ready` after hydration; both automatic and explicit connection wait for it. This phase is implemented and verified locally.

The saved records already contain optimistic state, atomically checkpointed with the bases and outbox. Startup restores that checkpoint directly; it does not rebuild the records or replay mutations again. Install the persistence subscriber after loading so hydration does not enqueue a redundant save. Initialize replica metadata only when the checkpoint has none.

```callstack
 TandemClient [[packages/core/src/TandemClient.ts#TandemClient]]
+├── Database.loadFromStorage # load every checkpoint namespace [[database4:new:244-247]] [[database4:new:249]]
+├── initialize replica metadata only for a fresh store [[database4:new:208-211]]
+├── resolve ready after hydration [[database4:new:212-215]]
 └── SyncEngine.connect [[packages/core/src/sync/SyncEngine.ts#SyncEngine.connect]]
+   ├── await hydration before connecting [[sync4:new:176]]
    ├── SyncEngine.queuePull # reconcile and checkpoint the server acknowledgement [[packages/core/src/sync/SyncEngine.ts#SyncEngine.queuePull]]
    └── SyncEngine.push # deliver remaining mutations with their original IDs [[packages/core/src/sync/SyncEngine.ts#SyncEngine.push]]
```

`transact()` stays synchronous and throws before readiness [[client4:new:154-156]], so a draft cannot read the empty pre-hydration database and overwrite restored data. Clients without storage remain immediately writable. `clear()` waits for hydration before discarding the saved session [[client4:new:214]]. A failed storage scan rejects readiness and connection without replacing the checkpoint. Await `ready` before using the saved `clientId`; the public getter returns the generated fallback while hydration is pending. `Database.replica` checks its initialized-replica invariant once [[database4:new:137-140]], so sync callers do not need non-null assertions. Only initialization performs an optional tuple lookup.

```tsx
await client.ready // Restore records, bases, outbox, identity, counter, and cookie.
const tx = client.transact() // Throws if created before hydration finishes.

// Existing reconnect order, now using the restored session:
await remote.connect({ clientId: db.replica.clientId, poke })
await pull() // Prune accepted IDs, reconcile the patch, and persist the checkpoint.
await push() // Send only mutations still pending, in their original order.
```

Keep the existing pull-first reconnect order. The server's acknowledgement prunes writes it already accepted; remaining mutations retain their original IDs. Existing server mutation deduplication handles retransmission, and a missing or mismatched cookie still requests a reset. `Database.applyPull` remains the only reconciliation/replay path. A reset retains unacknowledged writes and replays them on the new server base. The storage API and IndexedDB adapter do not change.

Build, type checks, lint, and all 162 tests pass. Seven new consumer tests use real IndexedDB to cover offline sets/removals, ordered operations, identity and counter continuity after an empty acknowledged outbox, lost push replies before and after pull checkpoint completion, automatic and explicit connection during delayed hydration, restored bases outside subscriptions, failed scans, and clear during startup. The offline restart regression fails against phase-3 code because it creates a new identity. The strengthened DST crash regression also fails there because the server loses the durable write; both pass with phase 4.

The DST crash fixture remains the historical trace, but its regression now asserts that the write reaches the server and both clients rather than checking convergence alone. Two 16-run sweeps of 500 steps passed: seeds 2026102200–2026102215 at fault rate 0.15, and seeds 2026102300–2026102315 at fault rate 0.1 and crash rate 0.05. Both reported zero violations, nondeterminism, and errors. There is still no automatic retry/backoff or coordination for multiple active clients sharing one store.

## References

- `packages/core/src/clientStorage/TandemClientStorage.ts` — tuple schema and storage contract.
- `packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts` — codec dispatch and IndexedDB batches.
- `packages/core/src/Database.ts` — transaction boundary and checkpoint hydration.
- `packages/core/src/sync/SyncEngine.ts` — delivery, acknowledgements, and connection lifecycle.
- `packages/core/src/utils/ThrottleQueue.ts` — ordered batching and flush barriers.
- `packages/core/test/persistence.spec.ts` — mixed-tuple and codec-failure regressions.
- `packages/core/src/clientStorage/TandemClientStorage.types.ts` — storage type contracts.
- `dst/DstWorld.ts` — simulation storage adapter using the same tuple schema.

## Source patches

Phase 4 patch excerpts come first, followed by phases 3, 2, and 1. Select linked call-stack rows to inspect old or new lines. Excerpts omit context lines so embedded patch indentation does not conflict with Markdown whitespace checks.

```source-diff:database4:packages/core/src/Database.ts
diff --git a/packages/core/src/Database.ts b/packages/core/src/Database.ts
index c915a6f..a9742dc 100644
--- a/packages/core/src/Database.ts
+++ b/packages/core/src/Database.ts
@@ -108,0 +109 @@ export class Database<
+	private initialized = false
@@ -110,0 +112,4 @@ export class Database<
+	get isReady() {
+		return this.initialized
+	}
+
@@ -132,2 +137,4 @@ export class Database<
-		// SyncEngine initializes this tuple before accepting synchronized operations.
-		return this.tupleDb.get(["replica"])!
+		// Sync operations run only after hydration has restored or initialized this tuple.
+		const replica = this.tupleDb.get(["replica"])
+		if (!replica) throw new Error("Replica is not initialized")
+		return replica
@@ -179,5 +187,15 @@ export class Database<
-			this.tupleDb.subscribe({}, (writes) => {
-				this.storageWrites = WriteOpsApi.merge(this.storageWrites, writes)
-				void this.persistClientStorage()?.then(
-					(result) => {
-						if (result instanceof Error)
+		}
+		const initialize = () => {
+			// Hydration restores an existing checkpoint; do not enqueue it for saving again.
+			if (this.clientStorage)
+				this.tupleDb.subscribe({}, (writes) => {
+					this.storageWrites = WriteOpsApi.merge(this.storageWrites, writes)
+					void this.persistClientStorage()?.then(
+						(result) => {
+							if (result instanceof Error)
+								this.logger.error({
+									message: "error committing to storage",
+									error: result,
+								})
+						},
+						(error) =>
@@ -186,9 +204,12 @@ export class Database<
-								error: result,
-							})
-					},
-					(error) =>
-						this.logger.error({
-							message: "error committing to storage",
-							error,
-						}),
-				)
+								error,
+							}),
+					)
+				})
+			if (initialClientId && !this.tupleDb.get(["replica"]))
+				this.initializeReplica(initialClientId)
+			this.initialized = true
+		}
+		if (this.clientStorage) {
+			this.ready = this.loadFromStorage(this.clientStorage).then((result) => {
+				if (result instanceof Error) throw result
+				initialize()
@@ -195,0 +217,3 @@ export class Database<
+		} else {
+			initialize()
+			this.ready = Promise.resolve()
@@ -197,3 +220,0 @@ export class Database<
-		this.ready = this.clientStorage
-			? this.loadFromStorage(this.clientStorage)
-			: Promise.resolve()
@@ -223,3 +244,4 @@ export class Database<
-		const records = await storage.scan(
-			normalizeTupleBounds({ prefix: ["record"] }),
-		)
+		const checkpoint = await storage
+			.scan()
+			.catch((cause) => new ClientStorageError({ operation: "scan", cause }))
+		if (checkpoint instanceof Error) return checkpoint
@@ -227 +249 @@ export class Database<
-		this.tupleDb.commit({ set: records as TandemClientTuple<AnySchema>[] })
+		this.tupleDb.commit({ set: checkpoint as TandemClientTuple<AnySchema>[] })
```

```source-diff:client4:packages/core/src/TandemClient.ts
diff --git a/packages/core/src/TandemClient.ts b/packages/core/src/TandemClient.ts
index c37b061..b65060c 100644
--- a/packages/core/src/TandemClient.ts
+++ b/packages/core/src/TandemClient.ts
@@ -148,0 +154,3 @@ export class TandemClient<
+		if (!this.db.isReady) {
+			throw new Error("Await client.ready before creating a transaction")
+		}
@@ -205,0 +214 @@ export class TandemClient<
+		if (!this.db.isReady) await this.ready
```

```source-diff:sync4:packages/core/src/sync/SyncEngine.ts
diff --git a/packages/core/src/sync/SyncEngine.ts b/packages/core/src/sync/SyncEngine.ts
index cabd3ef..d7be100 100644
--- a/packages/core/src/sync/SyncEngine.ts
+++ b/packages/core/src/sync/SyncEngine.ts
@@ -137 +135,0 @@ export class SyncEngine<Schema extends AnySchema> {
-		this.db.initializeReplica(args.clientId)
@@ -177,0 +176 @@ export class SyncEngine<Schema extends AnySchema> {
+		if (!this.db.isReady) await this.db.ready
```

```source-diff:database3:packages/core/src/Database.ts
diff --git a/packages/core/src/Database.ts b/packages/core/src/Database.ts
index 897841c..c915a6f 100644
--- a/packages/core/src/Database.ts
+++ b/packages/core/src/Database.ts
@@ -238 +283 @@ export class Database<
-	commitMutation(transaction: Transaction<Schema>, id: MutationId): void {
+	commitMutation(transaction: Transaction<Schema>): void {
@@ -239,0 +285,2 @@ export class Database<
+		const replica = this.replica
+		const id = tag<MutationId>(replica.lastIssuedMutationId + 1)
@@ -253,0 +301 @@ export class Database<
+		tx.set(["replica"], { ...replica, lastIssuedMutationId: id })
@@ -257 +305 @@ export class Database<
-	applyPull({ patch, lastMutationId }: PullResponse<Schema>): void {
+	applyPull({ patch, lastMutationId, cookie }: PullResponse<Schema>): void {
@@ -293,0 +342 @@ export class Database<
+		tx.set(["replica"], { ...this.replica, cookie })
```

```source-diff:sync3:packages/core/src/sync/SyncEngine.ts
diff --git a/packages/core/src/sync/SyncEngine.ts b/packages/core/src/sync/SyncEngine.ts
index c33c055..cabd3ef 100644
--- a/packages/core/src/sync/SyncEngine.ts
+++ b/packages/core/src/sync/SyncEngine.ts
@@ -268,13 +265,9 @@ export class SyncEngine<Schema extends AnySchema> {
-		// A synchronous subscriber can commit again before this call returns.
-		// Its ID must account for the outbox entry already published by the outer commit.
-		const id = tag<MutationId>(
-			Math.max(
-				this.mutationCount,
-				this.db.outbox.scan({ reverse: true, limit: 1 })[0]?.key[0] ?? 0,
-			) + 1,
-		)
-		this.db.commitMutation(transaction, id)
-		if (clientId === this.clientId)
-			this.mutationCount = Math.max(this.mutationCount, id)
-		this.logger.info({ message: "queueing push" })
-		return this.syncQueue.enqueue("push")
+		this.db.commitMutation(transaction)
+		const pendingCheckpoint = this.db.persistClientStorage()
+		if (!pendingCheckpoint) return this.syncQueue.enqueue("push")
+		return pendingCheckpoint.then((checkpoint) => {
+			if (checkpoint instanceof Error) throw checkpoint
+			if (clientId !== this.clientId) return
+			this.logger.info({ message: "queueing push" })
+			return this.syncQueue.enqueue("push")
+		})
@@ -289,0 +284,4 @@ export class SyncEngine<Schema extends AnySchema> {
+		// A queued push can capture mutations newer than the commit that requested it.
+		const checkpoint = await this.db.flushClientStorage()
+		if (checkpoint instanceof Error) return checkpoint
+		if (clientId !== this.clientId) return
```

```source-diff:database2:packages/core/src/Database.ts
diff --git a/packages/core/src/Database.ts b/packages/core/src/Database.ts
index 98f80ea..897841c 100644
--- a/packages/core/src/Database.ts
+++ b/packages/core/src/Database.ts
@@ -61,0 +107,20 @@ export class Database<
+	// tuple-database cannot reduce namespace filters over our generic mapped schema.
+	// The prefix fixes the namespace, so only these views assert its Schema-specific values.
+	private get records() {
+		return this.tupleDb.subspace(["record"]) as unknown as TupleDatabaseClient<
+			TupleClientRecord<Schema>
+		>
+	}
+
+	private get bases() {
+		return this.tupleDb.subspace(["base"]) as unknown as TupleDatabaseClient<
+			TupleClientBase<Schema>
+		>
+	}
+
+	get outbox() {
+		return this.tupleDb.subspace(["outbox"]) as unknown as TupleDatabaseClient<
+			TupleClientOutbox<Schema>
+		>
+	}
+
@@ -189,0 +238,85 @@ export class Database<
+	commitMutation(transaction: Transaction<Schema>, id: MutationId): void {
+		const tx = transaction.tupleDbTx
+		for (const op of transaction.ops) {
+			const recordKey = mutationOpToRecordKey(op)
+			// Pending bookkeeping reads the current checkpoint, not the user's draft read set.
+			const base = this.bases.get(recordKey)
+			// The draft already contains optimistic edits; capture the committed record instead.
+			const value = this.records.get(recordKey)
+			transaction.bases.set(recordKey, {
+				server:
+					base?.server ??
+					(value === undefined ? { type: "remove" } : { type: "set", value }),
+				lastWrittenBy: id,
+			})
+		}
+		transaction.outbox.set([id], [...transaction.ops])
+		tx.commit()
+	}
+
+	applyPull({ patch, lastMutationId }: PullResponse<Schema>): void {
+		const transaction = this.transact()
+		const tx = transaction.tupleDbTx
+		const { records, bases, outbox } = transaction
+		if (patch.reset) {
+			for (const { key } of records.scan()) records.remove(key)
+			for (const { key, value } of bases.scan()) {
+				bases.set(key, { ...value, server: { type: "remove" } })
+			}
+		}
+		for (const op of patch.set ?? [])
+			this.applyServerOp(transaction, { type: "set", ...op })
+		for (const op of patch.remove ?? [])
+			this.applyServerOp(transaction, { type: "remove", ...op })
+		for (const { key } of outbox.scan({ lte: [lastMutationId] }))
+			outbox.remove(key)
+		const trackedBases = bases.scan()
+		for (const { key, value } of trackedBases) {
+			switch (value.server.type) {
+				case "set":
+					records.set(key, value.server.value)
+					break
+				case "remove":
+					records.remove(key)
+					break
+				default:
+					unreachable(value.server)
+			}
+		}
+		// Preserve mutation and operation order; write() reorders removes before sets.
+		for (const { value: ops } of outbox.scan()) {
+			for (const op of ops) applyOp(transaction, op)
+		}
+		// Restoration still needs acknowledged bases, especially for records outside the scan window.
+		for (const { key, value } of trackedBases) {
+			if (value.lastWrittenBy <= lastMutationId) bases.remove(key)
+		}
+		tx.commit()
+	}
+
+	private applyServerOp(
+		transaction: Transaction<Schema>,
+		op: MutationOp<Schema>,
+	): void {
+		const recordKey = mutationOpToRecordKey(op)
+		const base = transaction.bases.get(recordKey)
+		if (base) {
+			const server: TandemClientBase<Schema[CollectionName<Schema>]>["server"] =
+				(() => {
+					switch (op.type) {
+						case "set":
+							return { type: "set", value: op.value }
+						case "remove":
+							return { type: "remove" }
+						default:
+							return unreachable(op)
+					}
+				})()
+			transaction.bases.set(recordKey, {
+				...base,
+				server,
+			})
+		}
+		applyOp(transaction, op)
+	}
+
```

```source-diff:sync2:packages/core/src/sync/SyncEngine.ts
diff --git a/packages/core/src/sync/SyncEngine.ts b/packages/core/src/sync/SyncEngine.ts
index 4100127..c33c055 100644
--- a/packages/core/src/sync/SyncEngine.ts
+++ b/packages/core/src/sync/SyncEngine.ts
@@ -265,4 +262,2 @@ export class SyncEngine<Schema extends AnySchema> {
-		const tx = this.db.makeTupleDbTransaction()
-		this.pendingWrites.applyPull(tx, { patch, lastMutationId })
-		tx.commit()
-		this.cookie = cookie
+		this.db.applyPull(response)
+		if (clientId === this.clientId) this.cookie = cookie
@@ -272,9 +267,12 @@ export class SyncEngine<Schema extends AnySchema> {
-		const mutation: Mutation<Schema> = {
-			ops: transaction.ops,
-			id: tag<MutationId>(this.mutationCount + 1),
-		}
-		this.pendingWrites.commitAndTrack(mutation, {
-			readCommittedRecord: ({ collection, id }) => this.db.get(collection, id),
-			commit: () => this.db.commit(transaction),
-		})
-		this.mutationCount += 1
+		const clientId = this.clientId
+		// A synchronous subscriber can commit again before this call returns.
+		// Its ID must account for the outbox entry already published by the outer commit.
+		const id = tag<MutationId>(
+			Math.max(
+				this.mutationCount,
+				this.db.outbox.scan({ reverse: true, limit: 1 })[0]?.key[0] ?? 0,
+			) + 1,
+		)
+		this.db.commitMutation(transaction, id)
+		if (clientId === this.clientId)
+			this.mutationCount = Math.max(this.mutationCount, id)
@@ -286 +284,5 @@ export class SyncEngine<Schema extends AnySchema> {
-		if (this.pendingWrites.isEmpty) return
+		const mutations = this.db.outbox.scan().map(({ key, value }) => ({
+			id: key[0],
+			ops: value,
+		}))
+		if (!mutations.length) return
@@ -294 +296 @@ export class SyncEngine<Schema extends AnySchema> {
-			mutations: this.pendingWrites.snapshot(),
+			mutations,
@@ -308 +309,0 @@ export class SyncEngine<Schema extends AnySchema> {
-		this.pendingWrites.clearAll()
```

```source-diff:transaction2:packages/core/src/transaction/Transaction.ts
diff --git a/packages/core/src/transaction/Transaction.ts b/packages/core/src/transaction/Transaction.ts
index 242ca68..2c9cfb6 100644
--- a/packages/core/src/transaction/Transaction.ts
+++ b/packages/core/src/transaction/Transaction.ts
@@ -140,5 +167,28 @@ export class Transaction<Schema extends AnySchema> {
-		/**
-		 * @internal
-		 */
-		readonly tupleDbTx: TupleRootTransactionApi<SchemaToTupleSchema<Schema>>,
-	) {}
+		tupleDbTx: TupleRootTransactionApi<TandemClientTuple<AnySchema>>,
+	) {
+		this.tupleDbTx = tupleDbTx
+	}
+
+	// tuple-database cannot reduce our generic mapped schema by prefix. Each
+	// subspace fixes the namespace; these assertions restore Schema to its values.
+	// These are views of tupleDbTx, not independent transactions.
+	/** @internal */
+	get records() {
+		return this.tupleDbTx.subspace([
+			"record",
+		]) as unknown as TupleTransactionApi<TupleClientRecord<Schema>>
+	}
+
+	/** @internal */
+	get bases() {
+		return this.tupleDbTx.subspace(["base"]) as unknown as TupleTransactionApi<
+			TupleClientBase<Schema>
+		>
+	}
+
+	/** @internal */
+	get outbox() {
+		return this.tupleDbTx.subspace([
+			"outbox",
+		]) as unknown as TupleTransactionApi<TupleClientOutbox<Schema>>
+	}
```

```source-diff:storage2:packages/core/src/clientStorage/TandemClientStorage.ts
diff --git a/packages/core/src/clientStorage/TandemClientStorage.ts b/packages/core/src/clientStorage/TandemClientStorage.ts
index d95a36b..fd953be 100644
--- a/packages/core/src/clientStorage/TandemClientStorage.ts
+++ b/packages/core/src/clientStorage/TandemClientStorage.ts
@@ -2 +2,6 @@ import type { KeyValuePair, ScanStorageArgs, WriteOps } from "tuple-database"
-import type { AnySchema, SchemaToTupleSchema } from "../schema/Schema.js"
+import type {
+	AnySchema,
+	CollectionIdTuple,
+	CollectionName,
+	SchemaToTupleSchema,
+} from "../schema/Schema.js"
@@ -6,0 +12,5 @@ import { isEqual } from "../utils/objectUtils.js"
+export type TandemClientBase<Record> = {
+	server: { type: "set"; value: Record } | { type: "remove" }
+	lastWrittenBy: MutationId
+}
+
@@ -9,0 +20,10 @@ export type TandemClientTuple<Schema extends AnySchema> =
+	| {
+			[Collection in CollectionName<Schema>]: {
+				key: [
+					"base",
+					Collection,
+					...CollectionIdTuple<Schema[Collection]["id"]>,
+				]
+				value: TandemClientBase<Schema[Collection]>
+			}
+	  }[CollectionName<Schema>]
```

```source-diff:indexeddb2:packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts
diff --git a/packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts b/packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts
index dec4c5f..0f29c9c 100644
--- a/packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts
+++ b/packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts
@@ -8,0 +9 @@ import type {
+	TandemClientBase,
@@ -74,0 +76,21 @@ function createTupleCodec<Schema extends AnySchema>(
+			case "base": {
+				const base = tuple.value as TandemClientBase<Schema[keyof Schema]>
+				switch (base.server.type) {
+					case "set": {
+						const valueCodec = codecs[tuple.key[1]]
+						return valueCodec
+							? {
+									...base,
+									server: {
+										type: "set",
+										value: valueCodec[direction](base.server.value),
+									},
+								}
+							: base
+					}
+					case "remove":
+						return base
+					default:
+						return unreachable(base.server)
+				}
+			}
```


```source-diff:storage:packages/core/src/clientStorage/TandemClientStorage.ts
diff --git a/packages/core/src/clientStorage/TandemClientStorage.ts b/packages/core/src/clientStorage/TandemClientStorage.ts
index e02b11e..d95a36b 100644
--- a/packages/core/src/clientStorage/TandemClientStorage.ts
+++ b/packages/core/src/clientStorage/TandemClientStorage.ts
@@ -1,12 +1,30 @@
 import type { KeyValuePair, ScanStorageArgs, WriteOps } from "tuple-database"
 import type { AnySchema, SchemaToTupleSchema } from "../schema/Schema.js"
+import type { ClientId, Cookie } from "../sync/SyncEngine.js"
+import type { Mutation, MutationId } from "../transaction/Transaction.js"
 import { isEqual } from "../utils/objectUtils.js"
 
+/** Durable replica tuples. Sync hydration and delivery are owned by SyncEngine. */
+export type TandemClientTuple<Schema extends AnySchema> =
+	| SchemaToTupleSchema<Schema>
+	| {
+			key: ["replica"]
+			value: {
+				clientId: ClientId
+				lastIssuedMutationId: MutationId
+				cookie?: Cookie
+			}
+	  }
+	| {
+			key: ["outbox", MutationId]
+			value: Mutation<Schema>["ops"]
+	  }
+
 export interface TandemClientStorageApi<Schema extends AnySchema> {
-	scan(args?: ScanStorageArgs): Promise<SchemaToTupleSchema<Schema>[]>
-	commit(writes: WriteOps<SchemaToTupleSchema<Schema>>): Promise<void>
+	scan(args?: ScanStorageArgs): Promise<TandemClientTuple<Schema>[]>
+	commit(writes: WriteOps<TandemClientTuple<Schema>>): Promise<void>
 	close(): Promise<void>
 	clear(): Promise<void>
 }
 
 export namespace WriteOpsApi {
@@ -61,20 +79,20 @@ export class TandemClientStorage<Schema extends AnySchema> {
 	constructor(
 		readonly adapter: TandemClientStorageApi<Schema>,
 		private readonly onFailure: (error: unknown) => void,
 	) {}
 
-	async commit(writeOps: WriteOps<SchemaToTupleSchema<Schema>>) {
+	async commit(writeOps: WriteOps<TandemClientTuple<Schema>>) {
 		try {
 			await this.adapter.commit(writeOps)
 		} catch (error) {
 			this.onFailure(error)
 			throw error
 		}
 	}
 
-	async scan(args?: ScanStorageArgs): Promise<SchemaToTupleSchema<Schema>[]> {
+	async scan(args?: ScanStorageArgs): Promise<TandemClientTuple<Schema>[]> {
 		// TODO: what should we do if this fails?
 		return await this.adapter.scan(args)
 	}
 
 	async clear() {
```

```source-diff:indexeddb:packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts
diff --git a/packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts b/packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts
index dba1788..dec4c5f 100644
--- a/packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts
+++ b/packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts
@@ -2,18 +2,18 @@
 
 import { deleteDB, openDB } from "idb"
 import type { DBSchema, IDBPDatabase } from "idb"
 import type { ScanStorageArgs, WriteOps } from "tuple-database"
 import { decodeTuple, encodeTuple } from "tuple-database/helpers/codec.js"
+import type { AnySchema, RuntimeSchemaDefinition } from "../schema/Schema.js"
 import type {
-	AnySchema,
-	RuntimeSchemaDefinition,
-	SchemaToTupleSchema,
-} from "../schema/Schema.js"
-import type { TandemClientStorageApi } from "./TandemClientStorage.js"
-import { Codec } from "../utils/Codec.js"
-import type { Json } from "../utils/typeUtils.js"
+	TandemClientStorageApi,
+	TandemClientTuple,
+} from "./TandemClientStorage.js"
+import type { MutationOp } from "../transaction/Transaction.js"
+import { codec, type Codec } from "../utils/Codec.js"
+import { unreachable, type Json } from "../utils/typeUtils.js"
 
 const version = 1
 
 const storeName = "tupledb"
 
@@ -58,33 +58,88 @@ function getSchemaCodecs<
 	) as {
 		[K in keyof Schema]?: Codec<Schema[K], StorageSchema[K]>
 	}
 }
 
+function createTupleCodec<Schema extends AnySchema>(
+	codecs: Record<string, Codec<unknown, unknown> | undefined>,
+): Codec<TandemClientTuple<Schema>, { key: string; value: unknown }> {
+	function transformValue(
+		tuple: TandemClientTuple<Schema>,
+		direction: "encode" | "decode",
+	): unknown {
+		switch (tuple.key[0]) {
+			case "record": {
+				const valueCodec = codecs?.[tuple.key[1]]
+				return valueCodec ? valueCodec[direction](tuple.value) : tuple.value
+			}
+			case "replica":
+				return tuple.value
+			case "outbox":
+				return (tuple.value as MutationOp<Schema>[]).map((op) => {
+					switch (op.type) {
+						case "set": {
+							const valueCodec = codecs?.[op.collection]
+							return valueCodec
+								? { ...op, value: valueCodec[direction](op.value) }
+								: op
+						}
+						case "remove":
+							return op
+						default:
+							return unreachable(op)
+					}
+				})
+			default:
+				return unreachable(tuple.key)
+		}
+	}
+	return codec(
+		"TandemClientTuple",
+		(input) => {
+			const tuple = input as TandemClientTuple<Schema>
+			return {
+				key: encodeTuple(tuple.key),
+				value: transformValue(tuple, "encode"),
+			}
+		},
+		(input) => {
+			const encoded = input as { key: string; value: unknown }
+			const tuple = {
+				key: decodeTuple(encoded.key),
+				value: encoded.value,
+			} as TandemClientTuple<Schema>
+			return {
+				key: tuple.key,
+				value: transformValue(tuple, "decode"),
+			} as TandemClientTuple<Schema>
+		},
+	)
+}
+
 export class TandemClientIndexedDbStorage<
 	Schema extends AnySchema,
 	StorageSchema extends {
 		[K in keyof Schema]?: Json
 	} = Schema,
 > implements TandemClientStorageApi<Schema> {
 	private db: Promise<IDBPDatabase<TandemIndexedDbSchema>>
-	private codecs?: {
-		[K in keyof Schema]?: Codec<Schema[K], StorageSchema[K]>
-	}
+	private readonly tupleCodec: Codec<
+		TandemClientTuple<Schema>,
+		{ key: string; value: unknown }
+	>
 	private dbName: string
 
 	constructor({
 		dbName,
 		schema,
 		codecs,
 	}: TandemClientIndexedDbStorageArgs<Schema, StorageSchema>) {
-		this.codecs = {
+		this.tupleCodec = createTupleCodec<Schema>({
 			...getSchemaCodecs(schema),
 			...codecs,
-		} as {
-			[K in keyof Schema]?: Codec<Schema[K], StorageSchema[K]>
-		}
+		})
 		this.dbName = dbName
 		this.db = openDB<TandemIndexedDbSchema>(dbName, version, {
 			upgrade(db) {
 				db.createObjectStore(storeName)
 			},
@@ -123,38 +178,33 @@ export class TandemClientIndexedDbStorage<
 		}
 
 		const direction = args?.reverse ? "prev" : "next"
 
 		const limit = args?.limit || Infinity
-		const results: SchemaToTupleSchema<Schema>[] = []
+		const results: TandemClientTuple<Schema>[] = []
 		for await (const cursor of index.iterate(range, direction)) {
-			const key = decodeTuple(cursor.key)
-			const recordType = key[1] as keyof Schema & string
-			const codec = this.codecs?.[recordType]
-			const value = codec ? codec.decode(cursor.value) : cursor.value
-
-			// IndexedDB and the tuple codec return untyped serialized data. The
-			// adapter is the boundary that restores the configured schema type.
-			results.push({ key, value } as SchemaToTupleSchema<Schema>)
+			results.push(
+				this.tupleCodec.decode({ key: cursor.key, value: cursor.value }),
+			)
 			if (results.length >= limit) break
 		}
 		await tx.done
 
 		return results
 	}
 
-	async commit(writes: WriteOps<SchemaToTupleSchema<Schema>>) {
+	async commit(writes: WriteOps<TandemClientTuple<Schema>>) {
+		// Finish codec work before opening a transaction so an encoding failure cannot persist a prefix.
+		const set = (writes.set ?? []).map((tuple) => this.tupleCodec.encode(tuple))
+		const remove = (writes.remove ?? []).map((key) => encodeTuple(key))
 		const db = await this.db
 		const tx = db.transaction(storeName, "readwrite")
-		for (const { key, value } of writes.set || []) {
-			const recordType = key[1] as keyof Schema & string
-			const codec = this.codecs?.[recordType]
-			const encodedValue = codec ? codec.encode(value) : value
-			await tx.store.put(encodedValue, encodeTuple(key))
+		for (const { key, value } of set) {
+			await tx.store.put(value, key)
 		}
-		for (const key of writes.remove || []) {
-			await tx.store.delete(encodeTuple(key))
+		for (const key of remove) {
+			await tx.store.delete(key)
 		}
 		await tx.done
 	}
 
 	async close() {
```

```source-diff:database:packages/core/src/Database.ts
diff --git a/packages/core/src/Database.ts b/packages/core/src/Database.ts
index 4f5caa3..98f80ea 100644
--- a/packages/core/src/Database.ts
+++ b/packages/core/src/Database.ts
@@ -1,5 +1,6 @@
+import { normalizeTupleBounds } from "tuple-database/helpers/sortedTupleArray.js"
 import {
 	InMemoryTupleStorage,
 	type ReadOnlyTupleDatabaseClientApi,
 	subscribeQuery,
 	TupleDatabase,
@@ -117,12 +118,14 @@ export class Database<
 	}: {
 		storage: TandemClientStorage<Schema>
 		timer: TimerApi
 	}) {
 		this.logger.info({ message: "loading from storage" })
-		const results = await storage.scan()
-		this.tupleDb.commit({ set: results })
+		const records = (await storage.scan(
+			normalizeTupleBounds({ prefix: ["record"] }),
+		)) as SchemaToTupleSchema<Schema>[]
+		this.tupleDb.commit({ set: records })
 
 		let writeOpsQueue: WriteOps<SchemaToTupleSchema<Schema>> = {}
 
 		const clientStorageWriteQueue = new ThrottleQueue(async () => {
 			this.logger.info({ message: "committing to storage" })
```

```source-diff:tests:packages/core/test/persistence.spec.ts
diff --git a/packages/core/test/persistence.spec.ts b/packages/core/test/persistence.spec.ts
index c5e55b6..735ff4a 100644
--- a/packages/core/test/persistence.spec.ts
+++ b/packages/core/test/persistence.spec.ts
@@ -3,15 +3,20 @@ import {
 	codec,
 	collection,
 	defineSchema,
 	type LoggerApi,
 	type RemoteApi,
+	tag,
+	type ClientId,
+	type Cookie,
+	type MutationId,
 	TandemClient,
 	type TandemClientStorageApi,
+	type TandemClientTuple,
 } from "@tanishqkancharla/tandem-core"
 import * as errore from "errore"
-import { describe, expect } from "vitest"
+import { assert, describe, expect } from "vitest"
 import {
 	buildGatekeeperHarness,
 	expectQuery,
 	test,
 	todo,
@@ -97,10 +102,133 @@ function createPersistentSyncGatekeeper({
 	})
 	return { clients, gatekeeper }
 }
 
 describe("TandemClient persistence", () => {
+	test("round-trips replica metadata and ordered outbox operations alongside codec-backed records", async ({
+		makeStorage,
+		rng,
+		logger,
+	}) => {
+		const dbName = rng.next("replica-tuples")
+		const storage = makeStorage<TestsEventSchema>({
+			dbName,
+			schema: testsEventRuntimeSchema,
+		})
+		const event = {
+			id: "event-1",
+			title: "Confirmed",
+			startAt: new EventStart("2026-05-04T12:00:00.000Z"),
+		}
+		const edited = {
+			...event,
+			title: "Pending",
+			startAt: new EventStart("2026-05-05T16:30:00.000Z"),
+		}
+		const mutationId = tag<MutationId>(8)
+		const tuples: TandemClientTuple<TestsEventSchema>[] = [
+			{ key: ["record", "events", event.id], value: event },
+			{
+				key: ["replica"],
+				value: {
+					clientId: tag<ClientId>("client-a"),
+					lastIssuedMutationId: mutationId,
+					cookie: tag<Cookie>("view-b"),
+				},
+			},
+			{
+				key: ["outbox", mutationId],
+				value: [
+					{ type: "remove", collection: "events", id: "event-2" },
+					{ type: "set", collection: "events", value: edited },
+					{ type: "remove", collection: "events", id: event.id },
+				],
+			},
+		]
+		await storage.commit({ set: tuples })
+		await storage.close()
+
+		// A new adapter restores class instances inside records and outbox operations.
+		const reopened = makeStorage<TestsEventSchema>({
+			dbName,
+			schema: testsEventRuntimeSchema,
+		})
+		const restored = await reopened.scan()
+		expect(restored).toHaveLength(3)
+		expect(restored).toEqual(expect.arrayContaining(tuples))
+		const outbox = restored.find((tuple) => tuple.key[0] === "outbox")
+		assert(outbox && Array.isArray(outbox.value))
+		const set = outbox.value[1]
+		assert(set?.type === "set")
+		expect(set.value.startAt).toBeInstanceOf(EventStart)
+
+		// Until sync hydration is implemented, only application records enter the visible database.
+		await using harness = buildClientGatekeeper(
+			new TandemClient<TestsEventSchema>({
+				schema: testsEventRuntimeSchema,
+				clientStorage: reopened,
+				logger,
+				autoConnect: false,
+			}),
+		)
+		await harness.client.ready
+		expect(harness.client.query({ collection: "events" })).toEqual([event])
+
+		// Removing an outbox entry leaves records and metadata untouched.
+		await reopened.commit({ remove: [["outbox", mutationId]] })
+		expect(await reopened.scan()).toEqual(
+			expect.arrayContaining(tuples.slice(0, 2)),
+		)
+		expect(await reopened.scan()).toHaveLength(2)
+	})
+
+	test("does not persist a batch prefix when an outbox codec fails", async ({
+		makeStorage,
+		rng,
+	}) => {
+		const storage = makeStorage<TestsEventSchema>({
+			dbName: rng.next("failed-outbox-codec"),
+			codecs: {
+				events: codec(
+					"reject",
+					() => {
+						throw new Error("Cannot encode event")
+					},
+					(value) => value as TestsEvent,
+				),
+			},
+		})
+		await expect(
+			storage.commit({
+				set: [
+					{
+						key: ["replica"],
+						value: {
+							clientId: tag<ClientId>("client-a"),
+							lastIssuedMutationId: tag<MutationId>(1),
+						},
+					},
+					{
+						key: ["outbox", tag<MutationId>(1)],
+						value: [
+							{
+								type: "set",
+								collection: "events",
+								value: {
+									id: "event-1",
+									title: "Rejected",
+									startAt: new EventStart("2026-05-04"),
+								},
+							},
+						],
+					},
+				],
+			}),
+		).rejects.toThrow("Cannot encode event")
+		expect(await storage.scan()).toEqual([])
+	})
+
 	test("reloads persisted records after recreating the app", async ({
 		logger,
 		makeStorage,
 		rng,
 	}) => {
```

```source-diff:types:packages/core/src/clientStorage/TandemClientStorage.types.ts
diff --git a/packages/core/src/clientStorage/TandemClientStorage.types.ts b/packages/core/src/clientStorage/TandemClientStorage.types.ts
index 80fac96..6323537 100644
--- a/packages/core/src/clientStorage/TandemClientStorage.types.ts
+++ b/packages/core/src/clientStorage/TandemClientStorage.types.ts
@@ -1,6 +1,12 @@
-import { TandemClient } from "@tanishqkancharla/tandem-core"
+import {
+	TandemClient,
+	tag,
+	type ClientId,
+	type MutationId,
+	type Cookie,
+} from "@tanishqkancharla/tandem-core"
 import type { TandemClientStorageApi } from "@tanishqkancharla/tandem-core"
 
 type AppSchema = {
 	users: { id: string; name: string }
 }
@@ -14,5 +20,45 @@ declare const otherStorage: TandemClientStorageApi<OtherSchema>
 
 new TandemClient<AppSchema, {}>({ clientStorage: appStorage })
 
 // @ts-expect-error A client cache must store tuples for the client's schema.
 new TandemClient<AppSchema, {}>({ clientStorage: otherStorage })
+
+void appStorage.commit({
+	set: [
+		{
+			key: ["replica"],
+			value: {
+				clientId: tag<ClientId>("client"),
+				lastIssuedMutationId: tag<MutationId>(2),
+				cookie: tag<Cookie>("view"),
+			},
+		},
+		{
+			key: ["outbox", tag<MutationId>(2)],
+			value: [
+				{
+					type: "set",
+					collection: "users",
+					value: { id: "user", name: "Ada" },
+				},
+				{ type: "remove", collection: "users", id: "old-user" },
+			],
+		},
+	],
+})
+
+void appStorage.commit({
+	set: [
+		{
+			key: ["outbox", tag<MutationId>(3)],
+			value: [
+				{
+					type: "set",
+					// @ts-expect-error Outbox operations must name an application collection.
+					collection: "threads",
+					value: { id: "user", name: "Ada" },
+				},
+			],
+		},
+	],
+})
```

```source-diff:exports:packages/core/src/index.ts
diff --git a/packages/core/src/index.ts b/packages/core/src/index.ts
index 388624a..4908bc4 100644
--- a/packages/core/src/index.ts
+++ b/packages/core/src/index.ts
@@ -26,11 +26,14 @@ export type {
 	RemoveMutationOp,
 	SetMutationOp,
 } from "./transaction/Transaction.js"
 export { MutationApi } from "./transaction/Transaction.js"
 export { TandemClientStorage } from "./clientStorage/TandemClientStorage.js"
-export type { TandemClientStorageApi } from "./clientStorage/TandemClientStorage.js"
+export type {
+	TandemClientStorageApi,
+	TandemClientTuple,
+} from "./clientStorage/TandemClientStorage.js"
 export { WriteOpsApi } from "./clientStorage/TandemClientStorage.js"
 export { TandemClientIndexedDbStorage } from "./clientStorage/TandemClientIndexedDbStorage.js"
 export type {
 	AnyStorageSchema,
 	TandemClientIndexedDbStorageArgs,
```

```source-diff:dst:dst/DstWorld.ts
diff --git a/dst/DstWorld.ts b/dst/DstWorld.ts
index 64aec2a..44709b3 100644
--- a/dst/DstWorld.ts
+++ b/dst/DstWorld.ts
@@ -11,11 +11,11 @@ import {
 	Logger,
 	type RemoteApi,
 	type RngApi,
 	type RuntimeSchemaDefinition,
 	t,
-	type SchemaToTupleSchema,
+	type TandemClientTuple,
 	TandemClient,
 	type TandemClientStorageApi,
 	type TimerApi,
 	unreachable,
 } from "@tanishqkancharla/tandem-core"
@@ -94,11 +94,11 @@ function remoteWithPokeEvents(
 		push: (args) => server.push(args),
 		pull: (args) => server.pull(args),
 	}
 }
 
-type DstClientTuple = SchemaToTupleSchema<DstSchema>
+type DstClientTuple = TandemClientTuple<DstSchema>
 
 function roundTrip<Value>(value: Value): Value {
 	return JSON.parse(JSON.stringify(value)) as Value
 }
 
```
