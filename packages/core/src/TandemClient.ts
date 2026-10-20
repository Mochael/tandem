import { Database } from "./Database.js"
import {
	_encodeRelationalQuery,
	type RelationalQuery,
	type RelationalQueryResult,
} from "./query/Query.js"
import type {
	AnySchema,
	AnyRelations,
	RuntimeSchemaDefinition,
} from "./schema/Schema.js"
import type { TandemClientStorageApi } from "./clientStorage/TandemClientStorage.js"
import { SyncEngine, type ClientId, type RemoteApi } from "./sync/SyncEngine.js"
import { Transaction } from "./transaction/Transaction.js"
import { ConsoleLoggerSink, Logger, type LoggerApi } from "./utils/Logger.js"
import { randomId, type RngApi } from "./utils/randomId.js"
import type { TimerApi } from "./utils/Timer.js"
import type { AsyncUnsubscribe } from "./utils/typeUtils.js"

export type TandemClientArgs<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
> = {
	schema?: RuntimeSchemaDefinition<Schema>
	relations?: Relations
	/**
	 * Optional replica cache (IndexedDB tuples). Not the backend
	 * persistence adapter used by the server.
	 */
	clientStorage?: TandemClientStorageApi<Schema>
	/**
	 * Milliseconds between client storage writes, or a host-provided timer.
	 * @default 120
	 */
	clientStorageWriteInterval?: number | TimerApi
	remote?: RemoteApi<Schema>
	/**
	 * @default ConsoleLoggerSink
	 */
	logger?: LoggerApi
	rng?: RngApi
	/**
	 * Milliseconds between sync batches, or a host-provided timer.
	 * @default 150
	 */
	syncInterval?: number | TimerApi
	autoConnect?: boolean
}

export class TandemClient<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema> = AnyRelations<Schema>,
> {
	private readonly db: Database<Schema, Relations>
	/**
	 * Resolves after restoring the checkpoint and initializing replica metadata.
	 * With client storage configured, await this before creating transactions
	 * or using the restored clientId.
	 */
	readonly ready: Promise<void>

	// TODO: use a real uuid
	private readonly initialClientId: ClientId

	get clientId(): ClientId {
		// Await ready before using the saved identity; hydration may replace this fallback.
		return this.db.isReady && this.syncEngine
			? this.db.replica.clientId
			: this.initialClientId
	}

	private readonly syncEngine?: SyncEngine<Schema>
	private readonly logger: LoggerApi
	private readonly rng: RngApi

	constructor({
		schema,
		relations,
		clientStorage,
		clientStorageWriteInterval = 120,
		remote,
		logger,
		autoConnect = true,
		rng,
		syncInterval = 150,
	}: TandemClientArgs<Schema, Relations>) {
		this.rng = rng ?? { randomId }
		this.initialClientId = this.rng.randomId() as ClientId
		this.logger = logger ?? new Logger({ sinks: new ConsoleLoggerSink() })

		this.db = new Database({
			schema,
			relations,
			logger: this.logger.scope("db"),
			clientStorage,
			initialClientId: remote ? this.initialClientId : undefined,
			clientStorageWriteInterval,
			rng: this.rng,
		})

		this.syncEngine = remote
			? new SyncEngine({
					remote,
					db: this.db,
					autoConnect,
					logger: this.logger.scope("sync-engine"),
					rng: this.rng,
					syncInterval,
				})
			: undefined

		this.ready = this.db.ready
	}

	pullFromRemote(): Promise<void> {
		if (!this.syncEngine) {
			return Promise.resolve()
		}

		this.logger.info({ message: "pulling from remote" })
		return this.syncEngine.queuePull()
	}

	query<Query extends RelationalQuery<Schema, Relations>>(
		query: Query,
	): RelationalQueryResult<Schema, Relations, Query> {
		return this.db.query(query)
	}

	subscribe<Query extends RelationalQuery<Schema, Relations>>(
		query: Query,
		callback?: (
			result: RelationalQueryResult<Schema, Relations, Query>,
		) => void,
	): {
		result: RelationalQueryResult<Schema, Relations, Query>
		destroy: () => void
	} {
		const { result, destroy } = this.db.subscribe(query, callback ?? (() => {}))
		const unsubscribe = this.syncEngine?.subscribe(
			_encodeRelationalQuery(query.collection, query, this.db.relations),
		)

		return {
			result,
			destroy: () => {
				unsubscribe?.()
				destroy()
			},
		}
	}

	transact(): Transaction<Schema> {
		if (!this.db.isReady) {
			throw new Error("Await client.ready before creating a transaction")
		}
		return this.db.transact()
	}

	commit(transaction: Transaction<Schema>): Promise<void> {
		if (transaction.ops.length === 0) {
			this.logger.info({
				message: "attempted to commit transaction with no ops",
			})
			return Promise.resolve()
		}

		this.logger.info({ message: "committing transaction" })
		if (!this.syncEngine) {
			this.db.commit(transaction)
			return Promise.resolve()
		}
		const commitPromise = this.syncEngine.commit(transaction)

		// Ignored commit promises should not surface unhandled rejections.
		commitPromise.catch((error) => {
			this.logger.error({ message: "error pushing mutation", error })
		})

		return commitPromise
	}

	async connect(): Promise<AsyncUnsubscribe> {
		if (!this.syncEngine) {
			throw new Error("Attempted to connect without a remote server configured")
		}
		return await this.syncEngine.connect()
	}

	async disconnect() {
		if (!this.syncEngine) {
			this.logger.warn({
				message: "attempted to disconnect without a remote server configured",
			})
			return
		}
		return await this.syncEngine.disconnect()
	}

	/**
	 * Flush any pending writes to client storage immediately.
	 */
	async flushClientStorage(): Promise<void> {
		const result = await this.db.flushClientStorage()
		if (result instanceof Error) throw result
	}

	/**
	 * Discard local records and pending mutations and start a new sync identity.
	 * Does not undo server writes. Await this before starting new work.
	 */
	async clear() {
		this.logger.info({ message: "clearing database" })
		if (!this.db.isReady) await this.ready

		if (this.syncEngine) return this.syncEngine.clear()
		const result = await this.db.clear()
		if (result instanceof Error) throw result
	}
}
