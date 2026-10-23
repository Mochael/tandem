import * as errore from "errore"
import type { Database } from "../Database.js"
import type { EncodedQuery, ScanWindow } from "../query/Query.js"
import type { AnySchema, CollectionName } from "../schema/Schema.js"
import type {
	Mutation,
	MutationId,
	Transaction,
} from "../transaction/Transaction.js"
import type { LoggerApi } from "../utils/Logger.js"
import { randomId, type RngApi } from "../utils/randomId.js"
import { TaskQueue } from "../utils/TaskQueue.js"
import { Timer, type TimerApi } from "../utils/Timer.js"
import { tag } from "../utils/typeUtils.js"
import type {
	AsyncUnsubscribe,
	Tagged,
	Unsubscribe,
} from "../utils/typeUtils.js"

export type ClientId = Tagged<"ClientId", string>
export type Cookie = Tagged<"Cookie", number | string>

/** Expected failures are protocol data, not Error instances requiring revival. */
export type RemoteUnavailableError = { error: "unavailable"; message: string }
export type RemoteInvalidRequestError = {
	error: "invalid-request"
	message: string
}
export type RemoteMutationGapError = {
	error: "mutation-gap"
	expectedMutationId: number
	receivedMutationId: number
}
export type RemoteRequestError =
	| RemoteUnavailableError
	| RemoteInvalidRequestError
export type PushResponse =
	| { ok: true }
	| RemoteRequestError
	| RemoteMutationGapError
export type PullResponse<Schema extends AnySchema> = {
	cookie: Cookie
	patch: Patch<Schema>
	/** The last processed mutation, or 0 before any were applied. */
	lastMutationId: MutationId
}

class RemoteResponseError extends errore.createTaggedError({
	name: "RemoteResponseError",
	message: "Remote $operation failed",
}) {}

export type ClientApi = {
	clientId: ClientId
	/** Settles when the pull the poke started has finished. Never rejects. */
	poke: () => Promise<void>
}

export type RemoteApi<Schema extends AnySchema> = {
	connect(api: ClientApi): Promise<AsyncUnsubscribe | RemoteRequestError>
	push(args: {
		mutations: Mutation<Schema>[]
		clientId: ClientId
	}): Promise<PushResponse>
	pull(args: {
		clientId: ClientId
		cookie?: Cookie
		scanWindow: ScanWindow<Schema>
	}): Promise<PullResponse<Schema> | RemoteRequestError>
}

export type PatchSetOp<Schema extends AnySchema> = {
	[Collection in CollectionName<Schema>]: {
		collection: Collection
		value: Schema[Collection]
	}
}[CollectionName<Schema>]

export type PatchRemoveOp<Schema extends AnySchema> = {
	[Collection in CollectionName<Schema>]: {
		collection: Collection
		id: Schema[Collection]["id"]
	}
}[CollectionName<Schema>]

export type Patch<Schema extends AnySchema = AnySchema> = {
	/** Replace the confirmed replica with this snapshot before replaying pending writes. */
	reset?: boolean
	set?: PatchSetOp<Schema>[]
	remove?: PatchRemoveOp<Schema>[]
}

export namespace PatchApi {
	export function toString<Schema extends AnySchema>(
		patch: Patch<Schema>,
	): string {
		return `Patch {\n${
			patch.set
				?.map(
					(op) =>
						`  set ${op.collection}.${op.value.id} = ${JSON.stringify(op.value)}`,
				)
				.join("\n") ?? ""
		}\n${patch.remove?.map((op) => `  remove ${op.collection}.${op.id}`).join("\n") ?? ""}}`
	}
}

export type SyncEngineArgs<Schema extends AnySchema> = {
	remote: SyncEngine<Schema>["remote"]
	db: SyncEngine<Schema>["db"]
	autoConnect?: boolean
	logger: SyncEngine<Schema>["logger"]
	syncInterval: number | TimerApi
	rng?: RngApi
}

export class SyncEngine<Schema extends AnySchema> {
	private syncQueue: TaskQueue<"pull" | "push">
	private readonly db: Database<Schema>
	private readonly remote: RemoteApi<Schema>
	private readonly logger: LoggerApi
	private readonly rng: RngApi

	private disconnectFromRemote?: AsyncUnsubscribe
	// Advances on disconnect so a connect still in flight unregisters when it resolves.
	private connectionGeneration = 0
	private scanWindow: ScanWindow<Schema> = []

	get clientId(): ClientId {
		return this.db.replica.clientId
	}

	constructor(args: SyncEngineArgs<Schema>) {
		this.logger = args.logger
		this.remote = args.remote
		this.db = args.db
		this.rng = args.rng ?? { randomId }
		const timer =
			typeof args.syncInterval === "number"
				? new Timer({ interval: args.syncInterval })
				: args.syncInterval

		this.syncQueue = new TaskQueue(
			{
				pull: async () => {
					const result = await this.pull()
					if (result instanceof Error) throw result
				},
				push: async () => {
					const result = await this.push()
					if (result instanceof Error) throw result
				},
			},
			timer,
		)

		if (args.autoConnect) {
			this.connect().catch((error) => {
				// TODO: disconnect and operate in offline mode
				this.logger.error({ message: "error connecting to remote", error })
			})
		}
	}

	async connect(): Promise<AsyncUnsubscribe> {
		const connected = await this.connectToRemote()
		if (connected instanceof Error) throw connected
		await this.queuePull()
		if (this.db.outbox.scan({ limit: 1 }).length) {
			await this.syncQueue.enqueue("push")
		}
		return () => this.disconnect()
	}

	private async connectToRemote() {
		this.logger.info({ message: "connecting to remote" })
		const generation = this.connectionGeneration
		if (!this.db.isReady) await this.db.ready
		// Clear can rotate identity during either await; connect the new durable session instead.
		for (;;) {
			const clientId = this.clientId
			const checkpoint = await this.db.persistClientStorage()
			if (checkpoint instanceof Error) return checkpoint
			if (clientId !== this.clientId) continue
			const unsubscribe = await this.remote.connect({
				clientId,
				poke: () => {
					if (clientId !== this.clientId) return Promise.resolve()
					this.logger.info({ message: "received poke from remote" })
					return this.queuePull().catch((error) => {
						this.logger.error({ message: "error pulling from remote", error })
					})
				},
			})
			if (typeof unsubscribe !== "function") {
				return new RemoteResponseError({
					operation: "connect",
					cause: unsubscribe,
				})
			}
			if (generation !== this.connectionGeneration) {
				await unsubscribe()
				return
			}
			if (clientId !== this.clientId) {
				await unsubscribe()
				continue
			}
			// Overlapping connect calls keep only the newest registration.
			const previous = this.disconnectFromRemote
			this.disconnectFromRemote = unsubscribe
			await previous?.()

			this.logger.info({ message: "connected to remote" })
			return
		}
	}

	async disconnect() {
		this.logger.info({ message: "disconnecting from remote" })
		this.connectionGeneration += 1
		const unsubscribe = this.disconnectFromRemote
		this.disconnectFromRemote = undefined
		await unsubscribe?.()
	}

	subscribe(query: EncodedQuery<Schema>): Unsubscribe {
		this.logger.info({ message: "subscribing to query", query })
		this.scanWindow.push(query)
		void this.queuePull().catch((error) => {
			this.logger.error({ message: "error pulling from remote", error })
		})

		return () => {
			this.logger.info({ message: "unsubscribing from query", query })
			this.scanWindow.splice(this.scanWindow.indexOf(query), 1)
		}
	}

	queuePull(): Promise<void> {
		this.logger.info({ message: "queueing pull" })
		return this.syncQueue.enqueue("pull").then(() => {
			this.logger.info({ message: "pull finished" })
		})
	}

	private async pull() {
		if (!this.disconnectFromRemote) {
			return
		}

		this.logger.info({ message: "pulling from remote" })
		const clientId = this.clientId
		const response = await this.remote.pull({
			clientId,
			cookie: this.db.replica.cookie,
			scanWindow: this.scanWindow,
		})
		// A response from before clear() must not acknowledge the new session.
		if (clientId !== this.clientId) return
		if ("error" in response) {
			return new RemoteResponseError({ operation: "pull", cause: response })
		}
		const { cookie, patch, lastMutationId } = response

		this.logger.info({
			message: "pulled from remote",
			cookie,
			lastMutationId,
			patch: PatchApi.toString(patch),
		})

		this.db.applyPull(response)
		return this.db.persistClientStorage()
	}

	commit(transaction: Transaction<Schema>): Promise<void> {
		const clientId = this.clientId
		this.db.commitMutation(transaction)
		const pendingCheckpoint = this.db.persistClientStorage()
		if (!pendingCheckpoint) return this.syncQueue.enqueue("push")
		return pendingCheckpoint.then((checkpoint) => {
			if (checkpoint instanceof Error) throw checkpoint
			if (clientId !== this.clientId) return
			this.logger.info({ message: "queueing push" })
			return this.syncQueue.enqueue("push")
		})
	}

	private async push() {
		const clientId = this.clientId
		const mutations = this.db.outbox.scan().map(({ key, value }) => ({
			id: key[0],
			ops: value,
		}))
		if (!mutations.length) return

		// A queued push can capture mutations newer than the commit that requested it.
		const checkpoint = await this.db.flushClientStorage()
		if (checkpoint instanceof Error) return checkpoint
		if (clientId !== this.clientId) return
		if (!this.disconnectFromRemote) {
			this.logger.info({ message: "skipping push while disconnected" })
			return
		}

		const response = await this.remote.push({
			mutations,
			clientId,
		})
		if ("error" in response) {
			return new RemoteResponseError({ operation: "push", cause: response })
		}
	}

	async clear() {
		const unsubscribe = this.disconnectFromRemote
		this.disconnectFromRemote = undefined
		const [checkpoint] = await Promise.all([
			this.db.clear(tag<ClientId>(this.rng.randomId())),
			unsubscribe?.(),
		])
		if (checkpoint instanceof Error) throw checkpoint
		// Preserve connection state, but leave the database empty until a later pull.
		if (!unsubscribe) return
		const connected = await this.connectToRemote()
		if (connected instanceof Error) throw connected
	}
}
