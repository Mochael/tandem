import * as errore from "errore"
import {
	InMemoryTupleStorage,
	type ReadOnlyTupleDatabaseClientApi,
	subscribeQuery,
	TupleDatabase,
	TupleDatabaseClient,
	type WriteOps,
} from "tuple-database"
import {
	type AnySchema,
	type AnyRelations,
	type CollectionId,
	type CollectionName,
	collectionIdToTuple,
	type RuntimeSchemaDefinition,
	type SchemaToTupleSchema,
} from "./schema/Schema.js"
import {
	type RelationalQuery,
	type RelationalQueryResult,
	type ScanWindow,
} from "./query/Query.js"
import {
	executeQuerySync,
	executeScanWindowSync,
	type ScanWindowRecord,
} from "./query/executeQuery.js"
import {
	TandemClientStorage,
	type TandemClientBase,
	type TandemClientStorageApi,
	type TandemClientTuple,
	WriteOpsApi,
} from "./clientStorage/TandemClientStorage.js"
import {
	type MutationId,
	type MutationOp,
	type TupleClientBase,
	type TupleClientOutbox,
	type TupleClientRecord,
	Transaction,
} from "./transaction/Transaction.js"
import type { ClientId, PullResponse } from "./sync/SyncEngine.js"
import type { LoggerApi } from "./utils/Logger.js"
import type { RngApi } from "./utils/randomId.js"
import { ThrottleQueue } from "./utils/ThrottleQueue.js"
import { Timer, type TimerApi } from "./utils/Timer.js"
import { tag, unreachable, type Unsubscribe } from "./utils/typeUtils.js"

class ClientStorageError extends errore.createTaggedError({
	name: "ClientStorageError",
	message: "Client storage $operation failed",
}) {}

function mutationOpToRecordKey<Schema extends AnySchema>(
	op: MutationOp<Schema>,
): TupleClientRecord<Schema>["key"] {
	switch (op.type) {
		case "set":
			return [op.collection, ...collectionIdToTuple<CollectionId>(op.value.id)]
		case "remove":
			return [op.collection, ...collectionIdToTuple<CollectionId>(op.id)]
		default:
			return unreachable(op)
	}
}

function applyOp<Schema extends AnySchema>(
	transaction: Transaction<Schema>,
	op: MutationOp<Schema>,
): void {
	const key = mutationOpToRecordKey(op)
	switch (op.type) {
		case "set":
			transaction.records.set(key, op.value)
			return
		case "remove":
			transaction.records.remove(key)
			return
		default:
			return unreachable(op)
	}
}

export type DatabaseArgs<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
> = {
	schema?: RuntimeSchemaDefinition<Schema>
	relations?: Relations
	clientStorage?: TandemClientStorageApi<Schema>
	initialClientId?: ClientId
	clientStorageWriteInterval: number | TimerApi
	logger: LoggerApi
	rng: RngApi
}

export class Database<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema> = AnyRelations<Schema>,
> {
	// Root operations span every namespace; typed subspace views recover Schema below.
	private readonly tupleDb: TupleDatabaseClient<TandemClientTuple<AnySchema>>

	private readonly clientStorage?: TandemClientStorage<Schema>
	private readonly logger: LoggerApi
	private readonly rng: RngApi
	readonly schema?: RuntimeSchemaDefinition<Schema>
	readonly relations?: Relations
	private readonly clientStorageWriteQueue?: ThrottleQueue<void | ClientStorageError>
	private storageWrites: WriteOps<TandemClientTuple<AnySchema>> = {}
	private storageClearQueued = false
	private initialized = false
	readonly ready: Promise<void>

	get isReady() {
		return this.initialized
	}

	// tuple-database cannot reduce namespace filters over our generic mapped schema.
	// The prefix fixes the namespace, so only these views assert its Schema-specific values.
	private get records() {
		return this.tupleDb.subspace(["record"]) as unknown as TupleDatabaseClient<
			TupleClientRecord<Schema>
		>
	}

	private get bases() {
		return this.tupleDb.subspace(["base"]) as unknown as TupleDatabaseClient<
			TupleClientBase<Schema>
		>
	}

	get outbox() {
		return this.tupleDb.subspace(["outbox"]) as unknown as TupleDatabaseClient<
			TupleClientOutbox<Schema>
		>
	}

	private get sessions() {
		return this.tupleDb.subspace([
			"session",
		]) as unknown as TupleDatabaseClient<{
			key: [ClientId]
			value: { lastMutationId?: MutationId; scanWindow?: ScanWindow<Schema> }
		}>
	}

	get replica() {
		// Sync operations run only after hydration has restored or initialized this tuple.
		const replica = this.tupleDb.get(["replica"])
		if (!replica) throw new Error("Replica is not initialized")
		return replica
	}

	private initializeReplica(clientId: ClientId) {
		this.tupleDb.commit({
			set: [
				{
					key: ["replica"],
					value: { clientId, lastIssuedMutationId: tag<MutationId>(0) },
				},
			],
		})
	}

	constructor({
		logger,
		schema,
		relations,
		clientStorage,
		initialClientId,
		clientStorageWriteInterval,
		rng,
	}: DatabaseArgs<Schema, Relations>) {
		this.logger = logger
		this.schema = schema
		this.relations = relations
		this.clientStorage = clientStorage
			? new TandemClientStorage(clientStorage, (error) => {
					// TODO: clean up? What should we do when storage fails?
					this.logger.error({ message: "storage error", error })
				})
			: undefined

		this.rng = rng
		this.tupleDb = new TupleDatabaseClient(
			new TupleDatabase(new InMemoryTupleStorage(), { rng }),
		)

		const clientStorageWriteTimer =
			typeof clientStorageWriteInterval === "number"
				? new Timer({ interval: clientStorageWriteInterval })
				: clientStorageWriteInterval
		if (this.clientStorage) {
			this.clientStorageWriteQueue = new ThrottleQueue(
				() => this.persistStorageBatch(),
				clientStorageWriteTimer,
			)
		}
		const initialize = () => {
			// Hydration restores an existing checkpoint; do not enqueue it for saving again.
			if (this.clientStorage)
				this.tupleDb.subscribe({}, (writes) => {
					this.storageWrites = WriteOpsApi.merge(this.storageWrites, writes)
					void this.persistClientStorage()?.then(
						(result) => {
							if (result instanceof Error)
								this.logger.error({
									message: "error committing to storage",
									error: result,
								})
						},
						(error) =>
							this.logger.error({
								message: "error committing to storage",
								error,
							}),
					)
				})
			if (initialClientId && !this.tupleDb.get(["replica"]))
				this.initializeReplica(initialClientId)
			this.initialized = true
		}
		if (this.clientStorage) {
			this.ready = this.loadFromStorage(this.clientStorage).then((result) => {
				if (result instanceof Error) throw result
				initialize()
			})
		} else {
			initialize()
			this.ready = Promise.resolve()
		}
	}

	clear(clientId?: ClientId) {
		// Supersede queued old-session writes; an active batch finishes before the clear batch.
		this.storageWrites = {}
		this.storageClearQueued = true
		const tx = this.tupleDb.transact(this.rng.randomId())
		for (const { key } of tx.scan({})) tx.remove(key)
		if (clientId)
			tx.set(["replica"], {
				clientId,
				lastIssuedMutationId: tag<MutationId>(0),
			})
		tx.commit()
		return this.flushClientStorage()
	}

	/**
	 * What if values in storage changes?
	 * What if storage too big to load all at once?
	 */
	private async loadFromStorage(storage: TandemClientStorage<Schema>) {
		this.logger.info({ message: "loading from storage" })
		const checkpoint = await storage
			.scan()
			.catch((cause) => new ClientStorageError({ operation: "scan", cause }))
		if (checkpoint instanceof Error) return checkpoint
		// Restore Schema-specific storage values into the namespace-wide root view.
		this.tupleDb.commit({ set: checkpoint as TandemClientTuple<AnySchema>[] })
	}

	private async persistStorageBatch() {
		const writes = this.storageWrites
		const clear = this.storageClearQueued
		this.storageWrites = {}
		this.storageClearQueued = false
		await this.ready
		const result = await (async () => {
			if (clear) {
				const cleared = await this.clientStorage!.clear().catch(
					(cause) => new ClientStorageError({ operation: "clear", cause }),
				)
				if (cleared instanceof Error) return cleared
			}
			if (!writes.set?.length && !writes.remove?.length) return
			// The root spans all namespaces; storage restores the configured schema at its boundary.
			return this.clientStorage!.commit(
				writes as WriteOps<TandemClientTuple<Schema>>,
			).catch((cause) => new ClientStorageError({ operation: "commit", cause }))
		})()
		if (result instanceof Error && !this.storageClearQueued) {
			this.storageWrites = WriteOpsApi.merge(writes, this.storageWrites)
			this.storageClearQueued = clear
		}
		return result
	}

	persistClientStorage() {
		if (
			!this.storageClearQueued &&
			!this.storageWrites.set?.length &&
			!this.storageWrites.remove?.length
		)
			return this.clientStorageWriteQueue?.flush()
		return this.clientStorageWriteQueue?.enqueue()
	}

	/**
	 * Flush any pending writes to storage immediately.
	 */
	async flushClientStorage() {
		await this.ready
		void this.persistClientStorage()
		return this.clientStorageWriteQueue?.flush()
	}

	transact(): Transaction<Schema> {
		return new Transaction<Schema>(this.tupleDb.transact(this.rng.randomId()))
	}

	commit(transaction: Transaction<Schema>) {
		transaction.tupleDbTx.commit()
	}

	/**
	 * Commits a pending mutation. A leader forwarding a tab's mutation records
	 * that tab's mutation ID in the same transaction, so the forwarded write and
	 * its deduplication marker persist together.
	 */
	commitMutation(
		transaction: Transaction<Schema>,
		forwarded?: { clientId: ClientId; mutationId: MutationId },
	): void {
		const tx = transaction.tupleDbTx
		const replica = this.replica
		const id = tag<MutationId>(replica.lastIssuedMutationId + 1)
		for (const op of transaction.ops) {
			const recordKey = mutationOpToRecordKey(op)
			// Pending bookkeeping reads the current checkpoint, not the user's draft read set.
			const base = this.bases.get(recordKey)
			// The draft already contains optimistic edits; capture the committed record instead.
			const value = this.records.get(recordKey)
			transaction.bases.set(recordKey, {
				server:
					base?.server ??
					(value === undefined ? { type: "remove" } : { type: "set", value }),
				lastWrittenBy: id,
			})
		}
		transaction.outbox.set([id], [...transaction.ops])
		tx.set(["replica"], { ...replica, lastIssuedMutationId: id })
		if (forwarded)
			tx.set(["session", forwarded.clientId], {
				...this.sessions.get([forwarded.clientId]),
				lastMutationId: forwarded.mutationId,
			})
		tx.commit()
	}

	/** The last mutation this replica committed for a forwarding tab session. */
	sessionLastMutationId(clientId: ClientId): MutationId | undefined {
		return this.sessions.get([clientId])?.lastMutationId
	}

	/** Saves a tab session's queries, so a later leader keeps syncing them. */
	setSessionScanWindow(
		clientId: ClientId,
		scanWindow: ScanWindow<Schema> | undefined,
	) {
		const { scanWindow: _previous, ...session } =
			this.sessions.get([clientId]) ?? {}
		const tx = this.tupleDb.transact(this.rng.randomId())
		if (scanWindow) tx.set(["session", clientId], { ...session, scanWindow })
		else if (session.lastMutationId !== undefined)
			tx.set(["session", clientId], session)
		else tx.remove(["session", clientId])
		tx.commit()
	}

	sessionScanWindows(): {
		clientId: ClientId
		scanWindow: ScanWindow<Schema>
	}[] {
		return this.sessions
			.scan()
			.flatMap(({ key: [clientId], value: { scanWindow } }) =>
				scanWindow ? [{ clientId, scanWindow }] : [],
			)
	}

	/** Every record, for a tab session that starts without a checkpoint. */
	scanRecords(): ScanWindowRecord<Schema>[] {
		return this.records
			.scan()
			.map(
				({ key, value }) =>
					({ collection: key[0], value }) as ScanWindowRecord<Schema>,
			)
	}

	scanWindow(scanWindow: ScanWindow<Schema>) {
		// Scan windows only read the record namespace, like queries.
		return executeScanWindowSync<Schema, AnyRelations<Schema>>(
			this.tupleDb as unknown as ReadOnlyTupleDatabaseClientApi<
				SchemaToTupleSchema<Schema>
			>,
			this.relations ?? {},
			scanWindow,
		)
	}

	/** Calls back after each commit that writes records. */
	subscribeToRecords(callback: () => void): Unsubscribe {
		return this.tupleDb.subscribe({ prefix: ["record"] }, callback)
	}

	applyPull({ patch, lastMutationId, cookie }: PullResponse<Schema>): void {
		const transaction = this.transact()
		const tx = transaction.tupleDbTx
		const { records, bases, outbox } = transaction
		if (patch.reset) {
			for (const { key } of records.scan()) records.remove(key)
			for (const { key, value } of bases.scan()) {
				bases.set(key, { ...value, server: { type: "remove" } })
			}
		}
		for (const op of patch.set ?? [])
			this.applyServerOp(transaction, { type: "set", ...op })
		for (const op of patch.remove ?? [])
			this.applyServerOp(transaction, { type: "remove", ...op })
		for (const { key } of outbox.scan({ lte: [lastMutationId] }))
			outbox.remove(key)
		const trackedBases = bases.scan()
		for (const { key, value } of trackedBases) {
			switch (value.server.type) {
				case "set":
					records.set(key, value.server.value)
					break
				case "remove":
					records.remove(key)
					break
				default:
					unreachable(value.server)
			}
		}
		// Preserve mutation and operation order; write() reorders removes before sets.
		for (const { value: ops } of outbox.scan()) {
			for (const op of ops) applyOp(transaction, op)
		}
		// Restoration still needs acknowledged bases, especially for records outside the scan window.
		for (const { key, value } of trackedBases) {
			if (value.lastWrittenBy <= lastMutationId) bases.remove(key)
		}
		tx.set(["replica"], { ...this.replica, cookie })
		tx.commit()
	}

	private applyServerOp(
		transaction: Transaction<Schema>,
		op: MutationOp<Schema>,
	): void {
		const recordKey = mutationOpToRecordKey(op)
		const base = transaction.bases.get(recordKey)
		if (base) {
			const server: TandemClientBase<Schema[CollectionName<Schema>]>["server"] =
				(() => {
					switch (op.type) {
						case "set":
							return { type: "set", value: op.value }
						case "remove":
							return { type: "remove" }
						default:
							return unreachable(op)
					}
				})()
			transaction.bases.set(recordKey, {
				...base,
				server,
			})
		}
		applyOp(transaction, op)
	}

	subscribe<Query extends RelationalQuery<Schema, Relations>>(
		query: Query,
		callback: (result: RelationalQueryResult<Schema, Relations, Query>) => void,
	): {
		result: RelationalQueryResult<Schema, Relations, Query>
		destroy: () => void
	} {
		return subscribeQuery(
			this.tupleDb,
			(db) => this.executeQuery(db, query),
			callback,
		)
	}

	query<Query extends RelationalQuery<Schema, Relations>>(
		query: Query,
	): RelationalQueryResult<Schema, Relations, Query> {
		return this.executeQuery(this.tupleDb, query)
	}

	private executeQuery<Query extends RelationalQuery<Schema, Relations>>(
		db: ReadOnlyTupleDatabaseClientApi<TandemClientTuple<AnySchema>>,
		query: Query,
	): RelationalQueryResult<Schema, Relations, Query> {
		// Queries only read the record namespace; narrow the generic mapped tuple union here.
		return executeQuerySync<Schema, Relations, Query>(
			db as unknown as ReadOnlyTupleDatabaseClientApi<
				SchemaToTupleSchema<Schema>
			>,
			this.relations,
			query,
		)
	}
}
