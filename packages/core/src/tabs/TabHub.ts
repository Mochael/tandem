import type { Database } from "../Database.js"
import type { ScanWindow } from "../query/Query.js"
import type {
	AnyRelations,
	AnySchema,
	RuntimeSchemaDefinition,
} from "../schema/Schema.js"
import { collectionIdToTuple } from "../schema/Schema.js"
import type {
	ClientId,
	Cookie,
	PatchRemoveOp,
	PullResponse,
	PushResponse,
	RemoteRequestError,
	SyncEngine,
} from "../sync/SyncEngine.js"
import type { Mutation, MutationId } from "../transaction/Transaction.js"
import type { LoggerApi } from "../utils/Logger.js"
import type { RngApi } from "../utils/randomId.js"
import { tag, unreachable, type Unsubscribe } from "../utils/typeUtils.js"
import type { TabChannelApi, TabMessageListener } from "./TabEnvironment.js"
import { TabCodec, type TabMessage, type TabRequest } from "./TabProtocol.js"

/** How long a previous leader's sessions keep their queries without pulling again. */
const INHERITED_QUERY_GRACE_MS = 60_000

type SessionState<Schema extends AnySchema> = {
	cookie?: Cookie
	scanWindowKey?: string
	revision?: number
	/** Every record this session holds from the leader, by collection and ID. */
	sentRecords: Map<string, PatchRemoveOp<Schema>>
	/** The session's queries, added to the leader's own scan window. */
	subscriptions: Map<string, Unsubscribe>
	/** Restored from storage; this leader has not heard from the session yet. */
	inherited: boolean
}

export type TabHubArgs<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
> = {
	channel: TabChannelApi
	db: Database<Schema, Relations>
	syncEngine: SyncEngine<Schema>
	schema?: RuntimeSchemaDefinition<Schema>
	logger: LoggerApi
	rng: RngApi
}

function recordKey(collection: string, id: unknown) {
	return JSON.stringify([collection, collectionIdToTuple(id as never)])
}

function errorMessage(error: unknown) {
	return error instanceof Error ? error.message : String(error)
}

/**
 * Runs in the leader tab. It owns the persisted replica and its connection to
 * the remote, and serves every tab session the way a server serves clients.
 */
export class TabHub<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema> = AnyRelations<Schema>,
> {
	readonly leaderId: string
	private readonly channel: TabChannelApi
	private readonly db: Database<Schema, Relations>
	private readonly syncEngine: SyncEngine<Schema>
	private readonly schema?: RuntimeSchemaDefinition<Schema>
	private readonly logger: LoggerApi
	private readonly rng: RngApi
	private readonly sessions = new Map<ClientId, SessionState<Schema>>()
	/** Sessions that want the replica connected to the remote. */
	private readonly connectedSessions = new Set<ClientId>()
	private connection: Promise<void> = Promise.resolve()
	private revision = 0
	private pokeQueued = false
	private stopped = false
	private inheritedQueryTimer?: ReturnType<typeof setTimeout>
	private unsubscribeFromRecords?: Unsubscribe

	constructor(args: TabHubArgs<Schema, Relations>) {
		this.channel = args.channel
		this.db = args.db
		this.syncEngine = args.syncEngine
		this.schema = args.schema
		this.logger = args.logger
		this.rng = args.rng
		this.leaderId = args.rng.randomId()
	}

	/** Call after the replica loads and before it connects to the remote. */
	start() {
		// Keep pulling what the previous leader's sessions synced. A first pull
		// without their queries would remove those records from storage.
		for (const { clientId, scanWindow } of this.db.sessionScanWindows()) {
			const state = this.getSession(clientId)
			state.inherited = true
			state.scanWindowKey = JSON.stringify(scanWindow)
			this.subscribe(state, scanWindow)
		}
		this.inheritedQueryTimer = setTimeout(
			() => this.dropInheritedQueries(),
			INHERITED_QUERY_GRACE_MS,
		)
		this.channel.addEventListener("message", this.onMessage)
		this.unsubscribeFromRecords = this.db.subscribeToRecords(() => {
			this.revision += 1
			this.queuePoke()
		})
		this.post({ type: "leader", leaderId: this.leaderId })
		this.logger.info({ message: "leading tabs", leaderId: this.leaderId })
	}

	/** Stops serving sessions and leaves every write in storage for the next leader. */
	async stop() {
		this.stopped = true
		clearTimeout(this.inheritedQueryTimer)
		this.channel.removeEventListener("message", this.onMessage)
		this.channel.close()
		this.unsubscribeFromRecords?.()
		// Disconnecting first drops any pull still in flight, so nothing reaches storage after the flush.
		await this.syncEngine.disconnect()
		const flushed = await this.db.flushClientStorage()
		if (flushed instanceof Error)
			this.logger.error({
				message: "error flushing leader storage",
				error: flushed,
			})
	}

	private readonly onMessage: TabMessageListener = ({ data }) => {
		const message = data as TabMessage
		switch (message.type) {
			case "hello":
				this.post({ type: "leader", leaderId: this.leaderId })
				return
			case "request": {
				if (message.leaderId !== this.leaderId) return
				const { sessionId, requestId } = message
				const respond = (result: unknown) =>
					this.post({
						type: "response",
						leaderId: this.leaderId,
						sessionId,
						requestId,
						result,
					})
				void this.handle(sessionId, message.request as TabRequest<Schema>).then(
					respond,
					(error: unknown) => {
						this.logger.error({ message: "error serving tab request", error })
						respond({
							error: "unavailable",
							message: errorMessage(error),
						} satisfies RemoteRequestError)
					},
				)
				return
			}
			case "leader":
			case "poke":
			case "response":
				return
			default:
				return unreachable(message)
		}
	}

	private async handle(
		sessionId: ClientId,
		request: TabRequest<Schema>,
	): Promise<unknown> {
		switch (request.method) {
			case "push":
				return this.push(
					sessionId,
					TabCodec.decodeMutations(this.schema, request.mutations),
				)
			case "pull":
				return this.pull(sessionId, request.cookie, request.scanWindow)
			case "connect":
				this.connectedSessions.add(sessionId)
				await this.updateConnection()
				return { ok: true }
			case "disconnect":
				this.connectedSessions.delete(sessionId)
				await this.updateConnection()
				return { ok: true }
			case "pullFromRemote":
				await this.syncEngine.queuePull()
				return { ok: true }
			case "flush": {
				const flushed = await this.db.flushClientStorage()
				if (flushed instanceof Error) throw flushed
				return { ok: true }
			}
			case "clear":
				await this.syncEngine.clear()
				// Every session restarts from the cleared replica.
				for (const [clientId, state] of this.sessions) {
					state.cookie = undefined
					if (state.scanWindowKey)
						this.db.setSessionScanWindow(
							clientId,
							JSON.parse(state.scanWindowKey),
						)
				}
				this.revision += 1
				this.queuePoke()
				return { ok: true }
			case "leave":
				this.forget(sessionId)
				await this.updateConnection()
				return { ok: true }
			default:
				return unreachable(request)
		}
	}

	/** Connects the replica while any session wants it connected. */
	private updateConnection() {
		this.connection = this.connection
			.catch(() => {})
			.then(async () => {
				const wanted = !this.stopped && this.connectedSessions.size > 0
				// Syncing runs in the background, so a stalled remote push never blocks this chain.
				if (wanted && !this.syncEngine.isConnected)
					await this.syncEngine.connect({ waitForSync: false })
				// A connect that finished after stop() must not sync beside the next leader.
				const keep = !this.stopped && this.connectedSessions.size > 0
				if (!keep && this.syncEngine.isConnected)
					await this.syncEngine.disconnect()
			})
		return this.connection
	}

	private async push(
		sessionId: ClientId,
		mutations: Mutation<Schema>[],
	): Promise<PushResponse> {
		for (const mutation of mutations) {
			const lastMutationId = this.db.sessionLastMutationId(sessionId)
			// Resent after a leader change; storage already holds this mutation.
			if (lastMutationId !== undefined && mutation.id <= lastMutationId)
				continue
			// A session this replica has no marker for starts at whatever it sends first.
			if (lastMutationId !== undefined && mutation.id !== lastMutationId + 1)
				return {
					error: "mutation-gap",
					expectedMutationId: lastMutationId + 1,
					receivedMutationId: mutation.id,
				}

			const transaction = this.db.transact()
			for (const op of mutation.ops) {
				switch (op.type) {
					case "set":
						transaction.set(op.collection, op.value)
						break
					case "remove":
						transaction.remove(op.collection, op.id)
						break
					default:
						unreachable(op)
				}
			}
			// The replica's outbox retries a failed remote push, as it would for a single tab.
			this.syncEngine
				.commit(transaction, { clientId: sessionId, mutationId: mutation.id })
				.catch((error: unknown) => {
					this.logger.error({
						message: "error pushing forwarded mutation",
						error,
					})
				})
		}
		// The session's write is safe once storage holds it; do not wait for the network.
		const flushed = await this.db.flushClientStorage()
		if (flushed instanceof Error)
			return { error: "unavailable", message: flushed.message }
		return { ok: true }
	}

	private async pull(
		sessionId: ClientId,
		cookie: Cookie | undefined,
		scanWindow: ScanWindow<Schema>,
	): Promise<PullResponse<Schema> | RemoteRequestError> {
		const state = this.getSession(sessionId)
		state.inherited = false
		const scanWindowKey = JSON.stringify(scanWindow)
		if (scanWindowKey !== state.scanWindowKey) {
			this.subscribe(state, scanWindow)
			this.db.setSessionScanWindow(sessionId, scanWindow)
		}
		const lastMutationId =
			this.db.sessionLastMutationId(sessionId) ?? tag<MutationId>(0)
		const reset = cookie === undefined || cookie !== state.cookie
		let response: PullResponse<Schema>
		if (
			!reset &&
			state.scanWindowKey === scanWindowKey &&
			state.revision === this.revision
		) {
			response = { cookie, patch: {}, lastMutationId }
		} else {
			const records = this.db.scanWindow(scanWindow)
			if (records instanceof Error)
				return { error: "invalid-request", message: records.message }
			// A new session gets every record, as a single tab restores its whole
			// checkpoint. Later pulls narrow it to the scan window.
			const set = reset ? this.db.scanRecords() : records
			const sentRecords = new Map(
				set.map(({ collection, value }) => [
					recordKey(collection, value.id),
					{ collection, id: value.id } as PatchRemoveOp<Schema>,
				]),
			)
			const remove = reset
				? []
				: [...state.sentRecords]
						.filter(([key]) => !sentRecords.has(key))
						.map(([, op]) => op)
			state.cookie = tag<Cookie>(this.rng.randomId())
			state.scanWindowKey = scanWindowKey
			state.revision = this.revision
			state.sentRecords = sentRecords
			response = {
				cookie: state.cookie,
				patch: { ...(reset ? { reset: true } : {}), set, remove },
				lastMutationId,
			}
		}
		// Acknowledge only what storage holds, so a session never drops a write the leader could lose.
		const flushed = await this.db.flushClientStorage()
		if (flushed instanceof Error)
			return { error: "unavailable", message: flushed.message }
		return TabCodec.encodePull(this.schema, response)
	}

	private getSession(sessionId: ClientId): SessionState<Schema> {
		const existing = this.sessions.get(sessionId)
		if (existing) return existing
		const created: SessionState<Schema> = {
			sentRecords: new Map(),
			subscriptions: new Map(),
			inherited: false,
		}
		this.sessions.set(sessionId, created)
		return created
	}

	private subscribe(
		state: SessionState<Schema>,
		scanWindow: ScanWindow<Schema>,
	) {
		const queries = new Map(
			scanWindow.map((query) => [JSON.stringify(query), query]),
		)
		for (const [key, unsubscribe] of state.subscriptions) {
			if (queries.has(key)) continue
			unsubscribe()
			state.subscriptions.delete(key)
		}
		for (const [key, query] of queries) {
			if (!state.subscriptions.has(key))
				state.subscriptions.set(key, this.syncEngine.subscribe(query))
		}
	}

	private forget(sessionId: ClientId) {
		for (const unsubscribe of this.sessions
			.get(sessionId)
			?.subscriptions.values() ?? [])
			unsubscribe()
		this.sessions.delete(sessionId)
		this.connectedSessions.delete(sessionId)
		this.db.setSessionScanWindow(sessionId, undefined)
	}

	/** Sessions that never reached this leader belonged to closed tabs. */
	private dropInheritedQueries() {
		for (const [sessionId, state] of this.sessions) {
			if (state.inherited) this.forget(sessionId)
		}
	}

	private queuePoke() {
		if (this.pokeQueued) return
		this.pokeQueued = true
		queueMicrotask(() => {
			this.pokeQueued = false
			this.post({ type: "poke", leaderId: this.leaderId })
		})
	}

	private post(message: TabMessage) {
		this.channel.postMessage(message)
	}
}
