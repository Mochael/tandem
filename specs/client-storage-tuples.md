# Client storage tuples

This walkthrough covers persistence step 1: the storage-schema foundation. It does not describe durable offline writes as implemented. The embedded patches capture this implementation against its parent revision.

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

Keep the tuple-storage operations. Expand their accepted tuple schema and make codec dispatch distinguish the namespaces. No separate metadata store, outbox store, or new storage method is introduced.

```ts
// Simplified shape; actual declarations are linked below.
type ReplicaTuples =
  | { key: ["record", collection, ...id]; value: ApplicationRecord }
  | {
      key: ["replica"];
      value: { clientId: ClientId; lastIssuedMutationId: MutationId; cookie?: Cookie };
    }
  | { key: ["outbox", MutationId]; value: MutationOp[] };
```

The outbox key owns the mutation ID; its value holds operations in their original order. `lastIssuedMutationId` and the cookie remain distinct: one prevents mutation-ID reuse, while the other identifies a pulled view.

## Goals

- Represent records, replica metadata, and pending operations in one typed tuple schema.
- Round-trip custom application values both directly and inside outbox operations.
- Keep existing application queries and sync behavior unchanged in this first step.

## Non-goals

This change does not make `SyncEngine` persist or restore its identity, cookie, counter, or pending mutations. It does not implement confirmed-only record storage, startup replay, a durability promise for `commit()`, persistence-queue serialization, or automatic retries. The application still persists its visible records today.

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

### Phase 2: Connect the tuple schema to durable sync state

**Today**

`SyncEngine` and `PendingWrites` still own memory-only sync state. `Database` still persists visible application records through its existing queue.

**Proposed**

Use root tuple transactions for confirmed records, replica metadata, and the outbox; derive the visible records by replaying pending operations. Make persistence completion reliable before changing startup and delivery semantics. This is follow-up work, not part of the current diff.

The eventual lifecycle must restore state before connecting and persist a local mutation before sending it:

```callstack
 TandemClient [[packages/core/src/TandemClient.ts#TandemClient]]
+├── restore replica metadata and confirmed records
+├── rebuild PendingWrites from the outbox
+├── resolve ready after hydration
 └── SyncEngine.connect [[packages/core/src/sync/SyncEngine.ts#SyncEngine.connect]]
    ├── pull a server acknowledgement or reset
    └── push remaining durable mutations
```

A later pull must update confirmed records, prune the acknowledged outbox prefix, and save its cookie in one durable transaction. The existing server mutation deduplication and reset protocol remain the recovery mechanisms.

## References

- `packages/core/src/clientStorage/TandemClientStorage.ts` — tuple schema and storage contract.
- `packages/core/src/clientStorage/TandemClientIndexedDbStorage.ts` — codec dispatch and IndexedDB batches.
- `packages/core/src/Database.ts` — current record-cache loading and persistence.
- `packages/core/test/persistence.spec.ts` — mixed-tuple and codec-failure regressions.
- `packages/core/src/clientStorage/TandemClientStorage.types.ts` — storage type contracts.
- `dst/DstWorld.ts` — simulation storage adapter using the same tuple schema.

## Source patches

The following patches are copied from Git for the current working tree. Select linked call-stack rows to inspect the corresponding old or new lines.



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
