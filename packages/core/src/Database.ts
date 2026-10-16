import { normalizeTupleBounds } from "tuple-database/helpers/sortedTupleArray.js"
import {
	prependPrefixToWriteOps,
	removePrefixFromWriteOps,
} from "tuple-database/helpers/subspaceHelpers.js"
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
} from "./query/Query.js"
import { executeQuerySync } from "./query/executeQuery.js"
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
import type { PullResponse } from "./sync/SyncEngine.js"
import type { LoggerApi } from "./utils/Logger.js"
import type { RngApi } from "./utils/randomId.js"
import { ThrottleQueue } from "./utils/ThrottleQueue.js"
import { Timer, type TimerApi } from "./utils/Timer.js"
import { unreachable } from "./utils/typeUtils.js"

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
	private clientStorageWriteQueue?: ThrottleQueue
	readonly ready: Promise<void>

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

	constructor({
		logger,
		schema,
		relations,
		clientStorage,
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
		this.ready = this.clientStorage
			? this.loadFromStorage({
					storage: this.clientStorage,
					timer: clientStorageWriteTimer,
				})
			: Promise.resolve()
	}

	async clear() {
		const tx = this.tupleDb.transact(this.rng.randomId())
		for (const { key } of tx.scan({})) tx.remove(key)
		tx.commit()

		// Clear storage if available
		await this.clientStorage?.clear()
	}

	/**
	 * What if values in storage changes?
	 * What if storage too big to load all at once?
	 */
	private async loadFromStorage({
		storage,
		timer,
	}: {
		storage: TandemClientStorage<Schema>
		timer: TimerApi
	}) {
		this.logger.info({ message: "loading from storage" })
		const records = await storage.scan(
			normalizeTupleBounds({ prefix: ["record"] }),
		)
		// The scan selects record tuples; remove their namespace for the subspace.
		// The library's prefix helper preserves values but erases their tuple types.
		this.records.commit(
			removePrefixFromWriteOps(["record"], { set: records }) as WriteOps<
				TupleClientRecord<Schema>
			>,
		)

		let writeOpsQueue: WriteOps<TupleClientRecord<Schema>> = {}

		const clientStorageWriteQueue = new ThrottleQueue(async () => {
			this.logger.info({ message: "committing to storage" })
			const copy = writeOpsQueue
			writeOpsQueue = {}
			try {
				// Subspace subscriptions return relative keys. Restore the record prefix
				// at the storage boundary; the library's prefix helper erases tuple types.
				await storage.commit(
					prependPrefixToWriteOps(["record"], copy) as WriteOps<
						SchemaToTupleSchema<Schema>
					>,
				)
				this.logger.info({ message: "committed to storage" })
			} catch (error) {
				this.logger.error({ message: "error committing to storage", error })
				writeOpsQueue = copy
			}
		}, timer)

		this.clientStorageWriteQueue = clientStorageWriteQueue

		this.records.subscribe({}, (writeOps) => {
			writeOpsQueue = WriteOpsApi.merge(writeOpsQueue, writeOps)
			void clientStorageWriteQueue.enqueue()
		})
	}

	/**
	 * Flush any pending writes to storage immediately.
	 */
	async flushClientStorage(): Promise<void> {
		await this.clientStorageWriteQueue?.flush()
	}

	transact(): Transaction<Schema> {
		return new Transaction<Schema>(this.tupleDb.transact(this.rng.randomId()))
	}

	commit(transaction: Transaction<Schema>) {
		transaction.tupleDbTx.commit()
	}

	commitMutation(transaction: Transaction<Schema>, id: MutationId): void {
		const tx = transaction.tupleDbTx
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
		tx.commit()
	}

	applyPull({ patch, lastMutationId }: PullResponse<Schema>): void {
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
