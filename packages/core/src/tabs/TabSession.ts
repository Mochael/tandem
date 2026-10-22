import type {
	AnyRelations,
	AnySchema,
	RuntimeSchemaDefinition,
} from "../schema/Schema.js"
import type {
	ClientApi,
	ClientId,
	PullResponse,
	PushResponse,
	RemoteApi,
	RemoteRequestError,
} from "../sync/SyncEngine.js"
import type { LoggerApi } from "../utils/Logger.js"
import type { RngApi } from "../utils/randomId.js"
import { unreachable } from "../utils/typeUtils.js"
import type {
	TabChannelApi,
	TabEnvironmentApi,
	TabMessageListener,
} from "./TabEnvironment.js"
import type { TabHub } from "./TabHub.js"
import { TabCodec, type TabMessage, type TabRequest } from "./TabProtocol.js"

type PendingRequest = {
	message: Extract<TabMessage, { type: "request" }>
	resolve: (result: unknown) => void
}

export type TabSessionArgs<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
> = {
	name: string
	environment: TabEnvironmentApi
	schema?: RuntimeSchemaDefinition<Schema>
	logger: LoggerApi
	rng: RngApi
	/** Starts the persisted replica and its hub once this tab wins the lock. */
	lead: (channel: TabChannelApi) => Promise<TabHub<Schema, Relations>>
	/** Called when a new leader replaces one this session already used. */
	onLeaderChange: () => void
}

/**
 * One tab's view of the shared replica. Every tab, the leader included, syncs
 * its in-memory database with the leader through this remote, and waits to
 * become the leader itself.
 */
export class TabSession<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema> = AnyRelations<Schema>,
> implements RemoteApi<Schema> {
	private readonly channel: TabChannelApi
	private readonly schema?: RuntimeSchemaDefinition<Schema>
	private readonly logger: LoggerApi
	private readonly rng: RngApi
	private readonly onLeaderChange: () => void
	private readonly pending = new Map<string, PendingRequest>()
	private readonly pokes = new Set<ClientApi["poke"]>()
	private readonly released: Promise<void>
	private release!: () => void
	private readonly led: Promise<void>
	private leaderId?: string
	private leadError?: unknown
	private leading = false
	private closed = false

	constructor(args: TabSessionArgs<Schema, Relations>) {
		this.schema = args.schema
		this.logger = args.logger
		this.rng = args.rng
		this.onLeaderChange = args.onLeaderChange
		const name = `tandem:${args.name}`
		this.channel = args.environment.openChannel(name)
		this.channel.addEventListener("message", this.onMessage)
		this.channel.postMessage({ type: "hello" } satisfies TabMessage)

		this.released = new Promise((resolve) => (this.release = resolve))
		this.led = args.environment.locks
			.request(name, async () => {
				if (this.closed) return
				this.leading = true
				const hub = await args.lead(args.environment.openChannel(name))
				await this.released
				await hub.stop()
			})
			.catch((error: unknown) => {
				this.logger.error({ message: "error leading tabs", error })
				// No leader may follow, so fail requests as a single tab's failed hydration would.
				this.leading = false
				this.leadError = error
				for (const { resolve } of this.pending.values())
					resolve({
						error: "unavailable",
						message: error instanceof Error ? error.message : String(error),
					} satisfies RemoteRequestError)
				this.pending.clear()
			})
	}

	connect: RemoteApi<Schema>["connect"] = ({ poke }) => {
		this.pokes.add(poke)
		return Promise.resolve(() => {
			this.pokes.delete(poke)
			return Promise.resolve()
		})
	}

	push: RemoteApi<Schema>["push"] = async ({ clientId, mutations }) =>
		(await this.request(clientId, {
			method: "push",
			mutations: TabCodec.encodeMutations(this.schema, mutations),
		})) as PushResponse

	pull: RemoteApi<Schema>["pull"] = async ({
		clientId,
		cookie,
		scanWindow,
	}) => {
		const response = (await this.request(clientId, {
			method: "pull",
			cookie,
			scanWindow,
		})) as PullResponse<Schema> | RemoteRequestError
		if ("error" in response) return response
		return TabCodec.decodePull(this.schema, response)
	}

	/** Runs a client operation on the leader's replica. */
	async control(
		sessionId: ClientId,
		method: Exclude<TabRequest<Schema>["method"], "push" | "pull">,
	) {
		const result = (await this.request(sessionId, { method })) as
			| { ok: true }
			| RemoteRequestError
		if ("error" in result) throw new Error(result.message)
	}

	/** Leaves the leader, and hands leadership on once this tab's replica is saved. */
	async close(sessionId: ClientId) {
		if (this.closed) return
		this.closed = true
		// Best effort: the leader drops this session's queries from its scan window.
		if (this.leaderId !== undefined)
			this.channel.postMessage({
				type: "request",
				leaderId: this.leaderId,
				sessionId,
				requestId: this.rng.randomId(),
				request: { method: "leave" },
			} satisfies TabMessage)
		this.channel.removeEventListener("message", this.onMessage)
		this.channel.close()
		for (const { resolve } of this.pending.values())
			resolve({ error: "unavailable", message: "Tab session closed" })
		this.pending.clear()
		this.release()
		// A tab that never led leaves its lock request queued; it exits when granted.
		if (this.leading) await this.led
	}

	private request(
		sessionId: ClientId,
		request: TabRequest<Schema>,
	): Promise<unknown> {
		if (this.closed || (this.leaderId === undefined && this.leadError))
			return Promise.resolve({
				error: "unavailable",
				message: this.closed
					? "Tab session closed"
					: `No tab could lead: ${String(this.leadError)}`,
			} satisfies RemoteRequestError)
		const requestId = this.rng.randomId()
		return new Promise((resolve) => {
			const message = {
				type: "request",
				leaderId: this.leaderId ?? "",
				sessionId,
				requestId,
				request: request as TabRequest<AnySchema>,
			} satisfies TabMessage
			this.pending.set(requestId, { message, resolve })
			this.send(message)
		})
	}

	private send(message: PendingRequest["message"]) {
		// Requests wait until a leader announces itself.
		if (this.leaderId === undefined) return
		message.leaderId = this.leaderId
		this.channel.postMessage(message)
	}

	private readonly onMessage: TabMessageListener = ({ data }) => {
		const message = data as TabMessage
		switch (message.type) {
			case "leader": {
				if (message.leaderId === this.leaderId) return
				const previous = this.leaderId
				this.leaderId = message.leaderId
				this.logger.info({
					message: "following tab leader",
					leaderId: this.leaderId,
				})
				// A replaced leader may never answer. The new one skips pushes storage already holds.
				for (const { message: request } of this.pending.values())
					this.send(request)
				if (previous !== undefined) this.onLeaderChange()
				return
			}
			case "poke":
				if (message.leaderId !== this.leaderId) return
				for (const poke of this.pokes) void poke()
				return
			case "response": {
				if (message.leaderId !== this.leaderId) return
				const pending = this.pending.get(message.requestId)
				if (pending?.message.sessionId !== message.sessionId) return
				this.pending.delete(message.requestId)
				pending.resolve(message.result)
				return
			}
			case "hello":
			case "request":
				return
			default:
				return unreachable(message)
		}
	}
}
