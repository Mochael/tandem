# Authoritative storage sync

All three phases are implemented and verified. Every pull reads current storage. Adapters with change notifications can now trigger client pulls through the optional storage subscription. The JSON-file adapter has no watcher and still needs an existing pull trigger for external edits.

## Problem overview

Before Phase 2, external writes could leave subscribed clients stale indefinitely. `TandemServer.readPull` skipped scanning application records when its local revision, the client's cookie, and the subscription window had not changed. An external write changed none of those inputs, so manual pulls and reconnects kept returning empty patches.

The old pull path assumed every application write passed through Tandem:

```callstack
 TandemServer.pull [[packages/server/src/TandemServer.ts#TandemServer.pull]]
 └── TandemServer.readPull [[packages/server/src/TandemServer.ts#TandemServer.readPull]]
    ├── compare cookie, scan window, and server revision
    ├── skip scanning when all three match # miss external writes indefinitely
    └── compare previous and current record keys when scanning # derive removals
```

Before Phase 1, the JSON-file adapter added a separate freshness problem. `TandemServerJsonFileStorage.initialize` loaded the file once, and later scans read a long-lived in-memory copy. Even forcing a server scan could not discover a subsequent external file replacement. Phase 1 removes that cache.

A temporary two-client probe confirmed the server-level issue with externally visible storage: after an external insert, update, and deletion, both clients stayed stale through three manual pulls each and through disconnect/reconnect. An unrelated Tandem commit advanced the revision and made both clients converge on their next pulls. The probe passed and was removed; it is not a committed regression test.

## Solution overview

Read authoritative storage on every pull. Keep the existing per-client cookie and previously sent record keys so the server can still recover lost responses and calculate deletions. Remove revision-based scan skipping rather than adding another invalidation flag.

Allow adapters with change notifications to request client refreshes through an optional server-storage subscription. These notifications contain no records or operations. The server forwards them as existing client pokes, and clients pull their current subscription windows.

```diagram
┌─────────┐  change notification  ┌────────┐  poke  ┌────────┐
│ Storage │─────────────────────▶│ Server │───────▶│ Client │
└────▲────┘                      └───▲────┘        └───┬────┘
     │ read current records          │     pull        │
     └───────────────────────────────┴─────────────────┘
```

There is no safety polling in this design. With a notification-capable adapter, a delivered change notification prompts connected clients to pull. Without a notification, external changes become visible on the next manual pull, reconnect, or other existing pull trigger. An idle client that receives no notification has no automatic convergence guarantee.

The client-facing `RemoteApi`, mutation acknowledgements, optimistic replay, and lost-response reset protocol stay unchanged. This work does not change the existing policy that a pending local edit may recreate a deleted record.

## Goals

- Make every successful pull read the current records exposed by the storage adapter.
- Deliver externally inserted, updated, and deleted records without requiring an intervening Tandem write.
- Make JSON scans and write batches use current file contents rather than a persistent memory copy.
- Let notification-capable storage adapters prompt connected clients to refresh without changing the sync wire protocol.
- Preserve deletion diffs, lost-response recovery, and pending local writes.

## Non-goals

- Safety polling, periodic pulls, automatic network retries, or guaranteed delivery of storage notifications.
- Delete-wins semantics, tombstones, cascading deletion, or field-level mutation replay.
- A filesystem watcher for the JSON adapter or a database-specific change-data-capture implementation. The optional hook is exercised through a notification-capable test adapter; production adapters may omit it.
- Cross-process file locking or safe simultaneous JSON-file writers. Sequential external edits are supported; read-modify-rename does not make concurrent writers transactional.
- Making separate storage scans a single database snapshot, or adding live invalidation to server-side `TandemServer.subscribe` queries. This change concerns synchronized client views.

## Implementation

### ✅ Phase 1: Read current JSON-file contents per operation

Today

`TandemServerJsonFileStorage` retains `memory` and an `initialization` promise. Scans reuse that copy, and commits derive their next snapshot from it. An external edit can remain invisible and can be overwritten by a later Tandem write.

Proposed

Remove the persistent record copy and one-time initialization. Load the current file into an operation-local tuple store for each scan and each commit. Keep the existing per-instance operation queue, tagged file errors, missing-file-as-empty behavior, and temporary-file-plus-rename publication.

A scan loads the file before applying its requested tuple bounds. A commit reloads the latest file, applies its batch to that state, and publishes the replacement. An operation-local tuple store may still implement ordering and bounds; it is not retained between operations.

```callstack
 TandemServerJsonFileStorage.scan [[json-storage:new:59-68]]
-├── TandemServerJsonFileStorage.initialize [[json-storage:old:112-115]]
-└── scan the retained memory copy
+├── TandemServerJsonFileStorage.loadFromDisk # read current file contents [[json-storage:new:106-116]]
+└── TandemServerJsonFileStorage.scanMemory # apply requested bounds [[json-storage:new:66]]
 TandemServerJsonFileStorage.commit [[json-storage:new:70-78]]
-├── clone the retained memory copy
+├── TandemServerJsonFileStorage.loadFromDisk # include edits after the last scan [[json-storage:new:72-73]]
 ├── apply the write batch [[json-storage:new:75]]
 └── TandemServerJsonFileStorage.writeToDisk [[json-storage:new:76]]
```

The regression in [[packages/server/test/TandemServerJsonFileStorage.spec.ts]] uses two independent adapters over one real file. The first observes external updates, inserts, and removals through a bounded scan. The other writer then changes the file again, and the first commits an unrelated batch without overwriting those later changes. A freshly opened adapter verifies the published result. The malformed-file test also verifies that repairing the file makes the same adapter usable again.

Concurrent external writers must coordinate separately. The adapter continues to serialize only its own operations, and malformed external contents must reject rather than be treated as an empty database.

Verification passed: the updated JSON-storage tests failed against the original implementation and all four passed after the change. `pnpm run type-check`, `pnpm test` (164 tests), `pnpm run lint`, and a targeted formatting check passed. A disposable script against the built adapter also verified external file removal and recreation, refusing to overwrite malformed contents after initialization, recovery after repair, and preservation of queued commits. The following patch records the Phase 1 implementation.

```source-diff:json-storage:packages/server/src/storage/TandemServerJsonFileStorage.ts
diff --git a/packages/server/src/storage/TandemServerJsonFileStorage.ts b/packages/server/src/storage/TandemServerJsonFileStorage.ts
index 8c1cbe5..f682a7f 100644
--- a/packages/server/src/storage/TandemServerJsonFileStorage.ts
+++ b/packages/server/src/storage/TandemServerJsonFileStorage.ts
@@ -52,38 +52,28 @@ function isNotFoundError(value: unknown): boolean {
 export class TandemServerJsonFileStorage<
 	Schema extends AnySchema = AnySchema,
 > implements TandemServerStorageApi<Schema> {
-	private memory = new InMemoryTupleStorage()
-	private initialization:
-		| Promise<TandemServerJsonFileStorageError | undefined>
-		| undefined
 	private queue: Promise<void> = Promise.resolve()
 
 	constructor(private readonly args: TandemServerJsonFileStorageArgs) {}
 
 	scan(args?: ScanStorageArgs): Promise<TandemTuple<Schema>[]> {
 		return this.run(async () => {
-			const initialized = await this.initialize()
-			if (initialized instanceof Error) return initialized
+			const memory = await this.loadFromDisk()
+			if (memory instanceof Error) return memory
 
 			// tuple-database's in-memory storage erases its tuple generic. The data
 			// comes from typed writes or a file previously written by this adapter.
-			return this.scanMemory(this.memory, args)
+			return this.scanMemory(memory, args)
 		})
 	}
 
 	commit(writes: WriteOps<TandemTuple<Schema>>): Promise<void> {
 		return this.run(async () => {
-			const initialized = await this.initialize()
-			if (initialized instanceof Error) return initialized
+			const memory = await this.loadFromDisk()
+			if (memory instanceof Error) return memory
 
-			const nextMemory = new InMemoryTupleStorage()
-			nextMemory.commit({ set: this.memory.scan() })
-			nextMemory.commit(writes)
-
-			const persisted = await this.writeToDisk(this.scanMemory(nextMemory))
-			if (persisted instanceof Error) return persisted
-
-			this.memory = nextMemory
+			memory.commit(writes)
+			return this.writeToDisk(this.scanMemory(memory))
 		})
 	}
 
@@ -109,22 +99,18 @@ export class TandemServerJsonFileStorage<
 		return result
 	}
 
-	private initialize() {
-		this.initialization ??= this.loadFromDisk()
-		return this.initialization
-	}
-
 	private scanMemory(memory: InMemoryTupleStorage, args?: ScanStorageArgs) {
 		return memory.scan(args) as TandemTuple<Schema>[]
 	}
 
 	private async loadFromDisk() {
+		const memory = new InMemoryTupleStorage()
 		const raw = await runFileOperation({
 			operation: "read",
 			filePath: this.args.filePath,
 			promise: fs.readFile(this.args.filePath, "utf8"),
 		})
-		if (raw instanceof Error && isNotFoundError(raw.cause)) return undefined
+		if (raw instanceof Error && isNotFoundError(raw.cause)) return memory
 		if (raw instanceof Error) return raw
 
 		const tuples = errore.try({
@@ -138,7 +124,8 @@ export class TandemServerJsonFileStorage<
 		})
 		if (tuples instanceof Error) return tuples
 
-		this.memory.commit({ set: tuples })
+		memory.commit({ set: tuples })
+		return memory
 	}
 
 	private async writeToDisk(tuples: TandemTuple<Schema>[]) {
```

### ✅ Phase 2: Make each pull authoritative

Today

`TandemServer.readPull` uses the server's local revision and cached scan-window key to decide whether to read records. Manual pulls and reconnects can therefore miss externally visible changes forever.

Proposed

Execute the requested scan window on every pull. Remove the server revision, per-client revision, and scan-window cache fields and helpers that exist only to skip scans. Retain scan-window validation, the per-client cookie, poke callback, and previously synchronized record keys.

Cookie comparison still decides whether a response is a reset or a diff. For a valid cookie, compare the previous keys with the current scan results to derive removals. For a missing or mismatched cookie, return the existing reset snapshot. Issue a fresh cookie for each generated view so losing any response still makes an old-cookie retry reset safely.

Every pull now executes the requested scan window. The local revision and cached window no longer gate storage reads:

```callstack
 TandemServer.pull [[packages/server/src/TandemServer.ts#TandemServer.pull]]
 └── TandemServer.readPull [[packages/server/src/TandemServer.ts#TandemServer.readPull]]
    ├── TandemServer.readLastMutationId # retain acknowledgement ordering
-   ├── skip scanning an unchanged revision and window [[authoritative-pull:old:382-388]]
+   ├── executeScanWindowAsync # read current records every time [[authoritative-pull:new:379-383]]
+   ├── compare previous and current record keys # derive removals [[authoritative-pull:new:392-397]]
+   └── publish fresh cookie and current keys # retain lost-response recovery [[authoritative-pull:new:398-405]]
```

The server publishes its new cookie and key history only after validation and reads succeed. The implementation keeps the existing queue and reset protocol:

```tsx
// Implemented behavior; error conversion and request validation are omitted here.
async function readPull(request) {
  const client = getSyncClient(request.clientId)
  const lastMutationId = await readLastMutationId(request.clientId)
  const reset = request.cookie === undefined || request.cookie !== client.cookie
  const records = await readCurrentScanWindow(request.scanWindow)
  const currentKeys = keysOf(records)
  const remove = reset ? [] : missingFrom(client.syncedRecordKeys, currentKeys)
  const cookie = newCookie()

  client.syncedRecordKeys = currentKeys
  client.cookie = cookie
  return {
    cookie,
    patch: { ...(reset ? { reset: true } : {}), set: records, remove },
    lastMutationId,
  }
}
```

The pseudocode names the behavior, not new helpers. Reads and publication stay within the existing server queue; acknowledgement ordering and error behavior are unchanged. Unchanged pulls resend current records and advance the cookie rather than returning a cached empty patch.

The regression in [[packages/server/test/TandemServer.spec.ts]] pulls a view, directly updates, inserts, and deletes backing records, then pulls with the same cookie and window. The response includes current records and the missing key. Retrying the old cookie simulates losing that deletion response and returns a reset with the authoritative records.

The two-client Gatekeeper regression in [[packages/core/test/sync/basic.spec.ts]] uses real JSON-file storage. One client pulls manually and the other reconnects after external updates, inserts, and removals. Both converge. Swapping refresh paths after removing every remaining record also clears both clients' views.

Both new regressions failed against the old server and pass with the change. `pnpm run type-check`, `pnpm test` (166 tests), and `pnpm run lint` pass, including lost-removal-response recovery, pending-write replay, relational queries, and paginated windows. The following patch records the Phase 2 implementation. Phase 3 notifications and safety polling are not part of this change.

```source-diff:authoritative-pull:packages/server/src/TandemServer.ts
diff --git a/packages/server/src/TandemServer.ts b/packages/server/src/TandemServer.ts
index 84990e9..cae1cd0 100644
--- a/packages/server/src/TandemServer.ts
+++ b/packages/server/src/TandemServer.ts
@@ -13,7 +13,6 @@ import type {
 	RemoteApi,
 	RemoteMutationGapError,
 	RngApi,
-	ScanWindow,
 	AnyRelations,
 	RuntimeSchemaDefinition,
 	SchemaToTupleSchema,
@@ -78,8 +77,6 @@ type SyncedRecordKey<
 type SyncClientState<Schema extends AnySchema> = {
 	poke?: ClientApi["poke"]
 	cookie?: Cookie
-	revision?: number
-	scanWindowKey?: string
 	syncedRecordKeys?: SyncedRecordKey<Schema>[]
 }
 
@@ -145,7 +142,6 @@ export class TandemServer<
 	private readonly syncClients = new Map<ClientId, SyncClientState<Schema>>()
 	private readonly tupleDb: AsyncTupleDatabaseClient<TandemTuple<Schema>>
 	private readonly rng?: RngApi
-	private revision = 0
 	// Tuple transactions batch writes but do not snapshot reads or serialize
 	// asynchronous storage commits. Order sync reads and commits here.
 	private queue: Promise<void> = Promise.resolve()
@@ -370,8 +366,8 @@ export class TandemServer<
 	> {
 		const { clientId, cookie, scanWindow } = args
 		const client = this.getSyncClient(clientId)
-		const scanWindowKey = this.encodeScanWindow(scanWindow)
-		if (scanWindowKey instanceof Error)
+		const serialized = errore.try(() => JSON.stringify(scanWindow))
+		if (serialized instanceof Error)
 			return {
 				error: "invalid-request",
 				message: "Scan window must be serializable",
@@ -379,18 +375,12 @@ export class TandemServer<
 		const lastMutationId = await this.readLastMutationId(clientId, "pull")
 		if (lastMutationId instanceof Error) return lastMutationId
 
-		const revision = this.revision
 		const shouldReset = cookie === undefined || cookie !== client.cookie
-		const scanWindowChanged = client.scanWindowKey !== scanWindowKey
-		const shouldRead =
-			shouldReset || scanWindowChanged || client.revision !== revision
-		const records = shouldRead
-			? await executeScanWindowAsync<Schema, Relations>(
-					this.recordDb,
-					this.relations,
-					scanWindow,
-				).catch((cause) => new TandemServerError({ operation: "pull", cause }))
-			: []
+		const records = await executeScanWindowAsync<Schema, Relations>(
+			this.recordDb,
+			this.relations,
+			scanWindow,
+		).catch((cause) => new TandemServerError({ operation: "pull", cause }))
 		if (records instanceof InvalidScanWindowError) {
 			return { error: "invalid-request", message: records.message }
 		}
@@ -399,35 +389,22 @@ export class TandemServer<
 		const currentRecordKeys = records.map((record) =>
 			scanWindowRecordToKey(record),
 		)
-		const remove: PatchRemoveOp<Schema>[] =
-			shouldRead && !shouldReset
-				? (client.syncedRecordKeys ?? []).filter(
-						(previous) => !containsRecordKey(currentRecordKeys, previous),
-					)
-				: []
-		client.scanWindowKey = scanWindowKey
-		if (shouldRead) {
-			client.syncedRecordKeys = currentRecordKeys
-			client.revision = revision
-			client.cookie = tag<Cookie>(this.rng?.randomId() ?? crypto.randomUUID())
-		}
+		const remove: PatchRemoveOp<Schema>[] = shouldReset
+			? []
+			: (client.syncedRecordKeys ?? []).filter(
+					(previous) => !containsRecordKey(currentRecordKeys, previous),
+				)
+		const nextCookie = tag<Cookie>(this.rng?.randomId() ?? crypto.randomUUID())
+		client.syncedRecordKeys = currentRecordKeys
+		client.cookie = nextCookie
 
 		return {
-			cookie: client.cookie!,
+			cookie: nextCookie,
 			patch: { ...(shouldReset ? { reset: true } : {}), set: records, remove },
 			lastMutationId,
 		}
 	}
 
-	private encodeScanWindow(
-		scanWindow: ScanWindow<Schema>,
-	): TandemServerError | string {
-		const result = errore.try(() => JSON.stringify(scanWindow))
-		return result instanceof Error
-			? new TandemServerError({ operation: "pull", cause: result })
-			: result
-	}
-
 	private async commitTransaction(
 		transaction: TandemServerTransaction<Schema, Relations>,
 		options: CommitOptions,
@@ -441,7 +418,6 @@ export class TandemServer<
 		if (hasWrites instanceof Error) return hasWrites
 		if (!hasWrites) return
 
-		this.revision += 1
 		this.emitPokes()
 	}
 
```

### ✅ Phase 3: Let storage changes request client refreshes

Today

`TandemServerStorageApi` exposes only `scan`, `commit`, and `close`. `TandemServer.commitTransaction` pokes clients after Tandem writes, but no storage-owned notification reaches that path.

Proposed

Add an optional synchronous `subscribe` method to the server-storage interface. It registers a callback and returns a synchronous unsubscribe function. The constructor registers once when the adapter supports it; `close` unregisters before closing storage. The subscription belongs to the server instance, not to each connected client.

The storage callback now reaches the same poke path as a Tandem commit. It requests client refreshes without resetting cookies or deleting previously synchronized record keys:

```callstack
 TandemServer.constructor
+└── TandemServerStorageApi.subscribe # register one backend listener [[storage-change-hook:new:23-29]]
+   └── TandemServer.emitPokes # request client pulls unless closing [[storage-pokes:new:159-162]]
 TandemServer.close
+├── mark the server closed # ignore notifications during teardown [[storage-pokes:new:281]]
+└── unsubscribe from storage # release the backend listener [[storage-pokes:new:282]]
```

The callback returns immediately. Closing marks the server closed before calling the unsubscribe function, so even an already queued notification cannot request new work during teardown:

```tsx
// Optional addition to TandemServerStorageApi.
interface StorageChangeNotifications {
  subscribe?(onChange: () => void): () => void
}

// Implemented server integration, in addition to existing commit-triggered pokes.
let closed = false
const unsubscribe = storage.subscribe?.(() => {
  if (closed) return
  emitPokes() // Request pulls; do not clear cookies or previously sent keys.
})

async function close() {
  closed = true
  unsubscribe?.()
  await closeExistingServerResources()
}
```

An adapter must signal after changed data becomes readable through `scan`, including changes made outside its own `commit` when its backend supports them. Notifications may be duplicate, broad, or coalesced. They do not identify rows, acknowledge mutations, or contain replayable writes. Adapters must not await a resulting client pull from inside a commit: the callback requests work and returns immediately.

No revision increment is needed after Phase 2 because every pull already scans. The server is invalidating the client's assumption that its view is current, not discarding the history needed for deletion diffs. Existing client `TaskQueue` behavior coalesces pending pulls and schedules a subsequent pull when a notification arrives during an in-flight one.

Pokes after Tandem commits remain unchanged for adapters without subscriptions. If an adapter also reports those writes, duplicate pokes are acceptable. Registration happens before the constructor returns, and the server releases the listener during cleanup. A backend connection that needs asynchronous setup should be prepared by the adapter before constructing the server; this hook does not introduce a new server readiness API.

The notification-capable fixture in [[packages/core/test/sync/basic.spec.ts]] wraps real JSON-file storage with a controllable change source. Two subscribed clients observe external inserts, updates, and deletions after notifications, without a manual pull after those writes. Duplicate notifications are harmless. Another regression holds an old pull response in flight, changes storage, and notifies; after that response arrives, the client performs another pull and converges.

The lifecycle regression in [[packages/server/test/TandemServer.spec.ts]] verifies one backend listener with multiple connected clients, deletion diffs after duplicate notifications, listener release, and ignored callbacks during and after close. All three new regressions fail against the Phase 2 server and pass with this change. `pnpm run type-check`, `pnpm test` (169 tests), `pnpm run lint`, and targeted formatting checks pass, including existing lost-response and pending-write replay coverage.

The implementation adds no polling, JSON-file watcher, client protocol messages, or invalidation for server-side query subscriptions. These patches record the optional adapter contract and server integration.

```source-diff:storage-change-hook:packages/server/src/storage/TandemServerStorage.ts
diff --git a/packages/server/src/storage/TandemServerStorage.ts b/packages/server/src/storage/TandemServerStorage.ts
index 4ac6541..bcfcb76 100644
--- a/packages/server/src/storage/TandemServerStorage.ts
+++ b/packages/server/src/storage/TandemServerStorage.ts
@@ -20,5 +20,13 @@ export interface TandemServerStorageApi<Schema extends AnySchema> {
 
 	commit(writes: WriteOps<TandemTuple<Schema>>): Promise<void>
 
+	/**
+	 * Optional change notifications, including external writes when supported.
+	 * Notify after changes are readable through scan. Duplicate notifications are
+	 * allowed. Callbacks request refreshes synchronously; do not await client pulls.
+	 * Return a synchronous unsubscribe function.
+	 */
+	subscribe?(onChange: () => void): () => void
+
 	close(): Promise<void>
 }
```

The server owns the listener alongside its other resources and stops notifications before closing them:

```source-diff:storage-pokes:packages/server/src/TandemServer.ts
diff --git a/packages/server/src/TandemServer.ts b/packages/server/src/TandemServer.ts
index cae1cd0..5dfc355 100644
--- a/packages/server/src/TandemServer.ts
+++ b/packages/server/src/TandemServer.ts
@@ -142,6 +142,8 @@ export class TandemServer<
 	private readonly syncClients = new Map<ClientId, SyncClientState<Schema>>()
 	private readonly tupleDb: AsyncTupleDatabaseClient<TandemTuple<Schema>>
 	private readonly rng?: RngApi
+	private readonly unsubscribeStorage?: () => void
+	private closed = false
 	// Tuple transactions batch writes but do not snapshot reads or serialize
 	// asynchronous storage commits. Order sync reads and commits here.
 	private queue: Promise<void> = Promise.resolve()
@@ -154,6 +156,10 @@ export class TandemServer<
 			{ rng: args.rng },
 		)
 		this.tupleDb = new AsyncTupleDatabaseClient<TandemTuple<Schema>>(database)
+		this.unsubscribeStorage = args.storage.subscribe?.(() => {
+			if (this.closed) return
+			this.emitPokes()
+		})
 	}
 
 	private get recordDb() {
@@ -272,6 +278,8 @@ export class TandemServer<
 	}
 
 	async close(): Promise<void> {
+		this.closed = true
+		this.unsubscribeStorage?.()
 		for (const destroy of this.subscriptions) destroy()
 		this.syncClients.clear()
 
```

## References

- [[packages/server/src/TandemServer.ts]]: pull generation, per-client state, serialized operations, and client pokes.
- [[packages/server/src/storage/TandemServerStorage.ts]]: server-storage contract.
- [[packages/server/src/storage/TandemServerJsonFileStorage.ts]]: JSON loading, operation queue, and atomic file replacement.
- [[packages/core/src/sync/SyncEngine.ts]]: subscriptions, reconnect, poke-triggered pulls, and remote protocol types.
- [[packages/core/src/utils/TaskQueue.ts]]: coalescing requested sync work; not periodic polling.
- [[packages/core/src/Database.ts#Database.applyPull]]: reset application, deletion patches, and ordered replay.
- [[packages/server/test/TandemServer.spec.ts]] and [[packages/server/test/TandemServerJsonFileStorage.spec.ts]]: server and JSON adapter coverage.
- [[packages/core/test/sync/conflicts.spec.ts]]: lost removal response and pending-write recovery.
