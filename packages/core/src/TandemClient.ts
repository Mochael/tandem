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
import {
	browserTabEnvironment,
	type TabEnvironmentApi,
} from "./tabs/TabEnvironment.js"
import { TabHub } from "./tabs/TabHub.js"
import { TabSession } from "./tabs/TabSession.js"
import { Transaction } from "./transaction/Transaction.js"
import { ConsoleLoggerSink, Logger, type LoggerApi } from "./utils/Logger.js"
import { randomId, type RngApi } from "./utils/randomId.js"
import type { TimerApi } from "./utils/Timer.js"
import { tag, type AsyncUnsubscribe } from "./utils/typeUtils.js"

export type TandemTabsOptions = {
	/**
	 * Clients with the same name share one replica across tabs.
	 * @default clientStorage.name
	 */
	name?: string
	/**
	 * Locks and channels shared by the tabs.
	 * @default Web Locks and BroadcastChannel in a browser document
	 */
	environment?: TabEnvironmentApi
}

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
	/**
	 * Share `clientStorage` with other tabs. One tab, the leader, owns the
	 * storage and the remote; every tab syncs an in-memory replica with it.
	 * On by default in browsers when the storage has a name and a remote is
	 * configured. `false` gives this client the storage to itself.
	 */
	tabs?: TandemTabsOptions | false
}

function resolveTabs<Schema extends AnySchema>(
	tabs: TandemTabsOptions | false | undefined,
	clientStorage: TandemClientStorageApi<Schema> | undefined,
	remote: RemoteApi<Schema> | undefined,
) {
	if (tabs === false || !remote || !clientStorage) return undefined
	const name = tabs?.name ?? clientStorage.name
	const environment = tabs?.environment ?? browserTabEnvironment()
	if (!tabs) return name && environment ? { name, environment } : undefined
	if (!name) throw new Error("tabs requires a name or a named clientStorage")
	if (!environment)
		throw new Error("tabs requires an environment outside browser documents")
	return { name, environment }
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
	private readonly tabSession?: TabSession<Schema, Relations>
	/** Whether this tab wants the leader connected to the remote. */
	private wantsRemote = false
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
		tabs,
	}: TandemClientArgs<Schema, Relations>) {
		this.rng = rng ?? { randomId }
		this.initialClientId = this.rng.randomId() as ClientId
		this.logger = logger ?? new Logger({ sinks: new ConsoleLoggerSink() })

		const sharedTabs = resolveTabs(tabs, clientStorage, remote)
		if (sharedTabs && remote && clientStorage) {
			// This tab's replica lives in memory. The leader tab, possibly this one,
			// holds the persisted replica and its remote connection.
			this.db = new Database({
				schema,
				relations,
				logger: this.logger.scope("db"),
				initialClientId: this.initialClientId,
				clientStorageWriteInterval,
				rng: this.rng,
			})
			this.tabSession = new TabSession<Schema, Relations>({
				...sharedTabs,
				schema,
				logger: this.logger.scope("tab-session"),
				rng: this.rng,
				lead: async (channel) => {
					const leaderDb = new Database({
						schema,
						relations,
						logger: this.logger.scope("leader-db"),
						clientStorage,
						initialClientId: tag<ClientId>(this.rng.randomId()),
						clientStorageWriteInterval,
						rng: this.rng,
					})
					// The hub connects once a tab asks, after restoring the queries tabs sync.
					const leaderSyncEngine = new SyncEngine({
						remote,
						db: leaderDb,
						autoConnect: false,
						logger: this.logger.scope("leader-sync-engine"),
						rng: this.rng,
						syncInterval,
					})
					await leaderDb.ready
					const hub = new TabHub({
						channel,
						db: leaderDb,
						syncEngine: leaderSyncEngine,
						schema,
						logger: this.logger.scope("tab-hub"),
						rng: this.rng,
					})
					hub.start()
					return hub
				},
				onLeaderChange: () => {
					// Pull from the new leader, then resend anything it has not acknowledged.
					const syncEngine = this.syncEngine!
					void syncEngine
						.disconnect()
						.then(() => syncEngine.connect())
						.then(() => this.requestRemote())
						.catch((error: unknown) => {
							this.logger.error({
								message: "error following new leader",
								error,
							})
						})
				},
			})
			this.syncEngine = new SyncEngine({
				remote: this.tabSession,
				db: this.db,
				autoConnect: false,
				logger: this.logger.scope("sync-engine"),
				rng: this.rng,
				syncInterval,
			})
			this.wantsRemote = autoConnect
			// Ready once the first pull from the leader restores the shared replica.
			this.ready = this.syncEngine.connect().then(() => {
				// Like a single tab's auto-connect, a failure is logged, not thrown.
				void this.requestRemote().catch((error: unknown) => {
					this.logger.error({ message: "error connecting to remote", error })
				})
			})
			return
		}

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
		if (this.tabSession)
			return this.tabSession
				.control(this.clientId, "pullFromRemote")
				.then(() => this.syncEngine!.queuePull())
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
		if (this.tabSession) {
			// Tabs stay connected to the leader. It stays connected to the remote while any tab asks.
			this.wantsRemote = true
			await this.requestRemote()
			return () => this.disconnect()
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
		if (this.tabSession) {
			this.wantsRemote = false
			return await this.tabSession.control(this.clientId, "disconnect")
		}
		return await this.syncEngine.disconnect()
	}

	/**
	 * Flush any pending writes to client storage immediately.
	 */
	async flushClientStorage(): Promise<void> {
		if (this.tabSession) {
			// The leader answers a push once its storage holds the writes.
			await this.syncEngine!.queuePush()
			return await this.tabSession.control(this.clientId, "flush")
		}
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

		if (this.tabSession) {
			const previous = this.clientId
			await this.tabSession.control(previous, "clear")
			await this.syncEngine!.clear()
			await this.requestRemote()
			await this.tabSession.control(previous, "leave")
			return await this.syncEngine!.queuePull()
		}
		if (this.syncEngine) return this.syncEngine.clear()
		const result = await this.db.clear()
		if (result instanceof Error) throw result
	}

	private async requestRemote() {
		if (this.wantsRemote)
			await this.tabSession!.control(this.clientId, "connect")
	}

	/**
	 * Stop syncing. A leader tab saves its replica and hands leadership to
	 * another tab before this resolves.
	 */
	async close() {
		if (!this.tabSession) return await this.disconnect()
		await this.flushClientStorage().catch((error: unknown) => {
			this.logger.error({ message: "error flushing before close", error })
		})
		await this.syncEngine!.disconnect()
		await this.tabSession.close(this.clientId)
	}
}
