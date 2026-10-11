import { normalizeTupleBounds } from "tuple-database/helpers/sortedTupleArray.js"
import {
	InMemoryTupleStorage,
	type ReadOnlyTupleDatabaseClientApi,
	subscribeQuery,
	TupleDatabase,
	TupleDatabaseClient,
	type TupleRootTransactionApi,
	type WriteOps,
} from "tuple-database"
import {
	type AnySchema,
	type AnyRelations,
	type CollectionIdTuple,
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
	type TandemClientStorageApi,
	WriteOpsApi,
} from "./clientStorage/TandemClientStorage.js"
import { Transaction } from "./transaction/Transaction.js"
import type { LoggerApi } from "./utils/Logger.js"
import type { RngApi } from "./utils/randomId.js"
import { ThrottleQueue } from "./utils/ThrottleQueue.js"
import { Timer, type TimerApi } from "./utils/Timer.js"

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
	private readonly tupleDb: TupleDatabaseClient<SchemaToTupleSchema<Schema>>

	private readonly clientStorage?: TandemClientStorage<Schema>
	private readonly logger: LoggerApi
	private readonly rng: RngApi
	readonly schema?: RuntimeSchemaDefinition<Schema>
	readonly relations?: Relations
	private clientStorageWriteQueue?: ThrottleQueue
	readonly ready: Promise<void>

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
		// Clear the in-memory tuple database
		const writeOps: WriteOps<SchemaToTupleSchema<Schema>> = {
			remove: this.tupleDb.scan({}).map(({ key }) => key),
		}

		if (writeOps.remove?.length) {
			this.tupleDb.commit(writeOps)
		}

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
		const records = (await storage.scan(
			normalizeTupleBounds({ prefix: ["record"] }),
		)) as SchemaToTupleSchema<Schema>[]
		this.tupleDb.commit({ set: records })

		let writeOpsQueue: WriteOps<SchemaToTupleSchema<Schema>> = {}

		const clientStorageWriteQueue = new ThrottleQueue(async () => {
			this.logger.info({ message: "committing to storage" })
			const copy = writeOpsQueue
			writeOpsQueue = {}
			try {
				await storage.commit(copy)
				this.logger.info({ message: "committed to storage" })
			} catch (error) {
				this.logger.error({ message: "error committing to storage", error })
				writeOpsQueue = copy
			}
		}, timer)

		this.clientStorageWriteQueue = clientStorageWriteQueue

		this.tupleDb.subscribe({}, (writeOps) => {
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

	makeTupleDbTransaction(): TupleRootTransactionApi<
		SchemaToTupleSchema<Schema>
	> {
		return this.tupleDb.transact(this.rng.randomId())
	}

	/** A record's committed value, read outside any open transaction. */
	get<Collection extends CollectionName<Schema>>(
		collection: Collection,
		id: Schema[Collection]["id"],
	): Schema[Collection] | undefined {
		// tuple-database cannot narrow a mapped tuple union while Schema is
		// generic, so read through the collection's subspace, as queries do.
		const records = this.tupleDb as unknown as {
			subspace(prefix: ["record", Collection]): {
				get(
					key: CollectionIdTuple<Schema[Collection]["id"]>,
				): Schema[Collection] | undefined
			}
		}
		return records.subspace(["record", collection]).get(collectionIdToTuple(id))
	}

	transact(): Transaction<Schema> {
		const tupleDbTx = this.tupleDb.transact(this.rng.randomId())
		return new Transaction(tupleDbTx)
	}

	commit(transaction: Transaction<Schema>) {
		transaction.tupleDbTx.commit()
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
		db: ReadOnlyTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
		query: Query,
	): RelationalQueryResult<Schema, Relations, Query> {
		return executeQuerySync<Schema, Relations, Query>(db, this.relations, query)
	}
}
