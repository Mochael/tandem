import {
	Gatekeeper,
	GatekeeperError,
	type CallHandle,
	type GatekeeperEvents,
	type Harness,
	type PendingCall,
} from "@tanishqkancharla/gatekeeper"
import {
	type ClientApi,
	type PokePayload,
	collection,
	defineMutators,
	type CollectionId,
	defineRelations,
	defineSchema,
	Logger,
	matchBy,
	MutationRejectedError,
	type MutationOp,
	type RelationalQuery,
	type RemoteApi,
	type RngApi,
	type RuntimeSchemaDefinition,
	type TandemClientTuple,
	TandemClient,
	type TandemClientStorageApi,
	type TimerApi,
	unreachable,
} from "@tanishqkancharla/tandem-core"
import {
	TandemServer,
	type ServerMutatorHooks,
	type TandemServerStorageApi,
	type TandemTuple,
} from "@tanishqkancharla/tandem-server"
import {
	InMemoryTupleStorage,
	type ScanStorageArgs,
	type WriteOps,
} from "tuple-database"
import * as errore from "errore"
import { isDeepStrictEqual } from "node:util"
import { SimPrng } from "./SimPrng.js"
import {
	DstTabGroup,
	type DstTabCoverage,
	type DstTabViolation,
} from "./DstTabs.js"
import { emptyCoverage, type DstCoverage } from "./DstCoverage.js"
import {
	expectedView,
	matchesTask,
	subscriptionQuery,
	type DstSubscriptionMode,
	type DstSubscriptionSpec,
} from "./DstSubscriptions.js"

export type DstSchema = {
	projects: { id: string; name: string }
	tasks: {
		id: string
		projectId: string
		text: string
		note?: string
		done: boolean
		priority: number
	}
	comments: { id: string; taskId: string; body: string }
	memberships: {
		id: readonly [projectId: string, memberId: string]
		role: string
	}
}

export const dstSchemaDefinition = defineSchema({
	projects: collection<DstSchema["projects"]>({ fields: ["id", "name"] }),
	tasks: collection<DstSchema["tasks"]>({
		fields: ["id", "projectId", "text", "note", "done", "priority"],
	}),
	comments: collection<DstSchema["comments"]>({
		fields: ["id", "taskId", "body"],
	}),
	memberships: collection<DstSchema["memberships"]>({ fields: ["id", "role"] }),
}) satisfies RuntimeSchemaDefinition<DstSchema>

/** Records only the server writes, as an agent's reply would be. */
export const serverOwnedIds = ["reply-1", "reply-2"]

export const dstMutators = defineMutators(dstSchemaDefinition, {
	appendTask: {
		args: (input: unknown) =>
			input as { id: string; text: string; field?: "text" | "note" },
		run: async (tx, { id, text, field }) => {
			const task = await tx.get("tasks", id)
			if (!task) return
			tx.set(
				"tasks",
				field === "note"
					? { ...task, note: (task.note ?? "") + text }
					: { ...task, text: task.text + text },
			)
		},
	},
})

type DstServer = TandemServer<
	DstSchema,
	typeof dstRelations,
	typeof dstMutators
>

function dstServerMutators(
	stats: DstStats,
): ServerMutatorHooks<DstSchema, typeof dstRelations, typeof dstMutators> {
	const effectKeys = new Set<string>()
	return {
		appendTask: {
			authorize: async (tx, { id }, ctx) => {
				if (ctx.source === "client" && serverOwnedIds.includes(id))
					throw new Error(`Only the server writes ${id}`)
				if (ctx.source === "client" && !(await tx.get("tasks", id)))
					throw new Error(`No task ${id}`)
			},
			afterCommit: (_args, ctx) => {
				if (ctx.source === "server") return
				const key = JSON.stringify([ctx.clientId, ctx.mutationId])
				if (effectKeys.has(key)) stats.effectRepeated = key
				effectKeys.add(key)
				stats.namedAppendCommits++
			},
		},
	}
}

export const dstRelations = defineRelations(
	dstSchemaDefinition,
	({ one, many }) => ({
		projects: { tasks: many("tasks", { from: "id", to: "projectId" }) },
		tasks: {
			project: one("projects", { from: "projectId", to: "id" }),
			comments: many("comments", { from: "id", to: "taskId" }),
		},
		comments: { task: one("tasks", { from: "taskId", to: "id" }) },
	}),
)

export const dstCollections = [
	"projects",
	"tasks",
	"comments",
	"memberships",
] as const

export type DstInventory = { [C in keyof DstSchema]: DstSchema[C][] }

class DstServerStorage implements TandemServerStorageApi<DstSchema> {
	private readonly memory = new InMemoryTupleStorage()
	private readonly external?: TandemServerStorageApi<DstSchema>

	constructor(external?: TandemServerStorageApi<DstSchema>) {
		this.external = external
	}

	/** Every value each server-owned record has had, in commit order; null when absent. */
	readonly history = new Map(serverOwnedIds.map((id) => [id, ["null"]]))

	scan(args?: ScanStorageArgs): Promise<TandemTuple<DstSchema>[]> {
		if (this.external) return this.external.scan(args)
		return Promise.resolve(this.memory.scan(args) as TandemTuple<DstSchema>[])
	}

	commit(writes: WriteOps<TandemTuple<DstSchema>>): Promise<void> {
		if (this.external)
			return this.external.commit(writes).then(() => this.recordHistory(writes))
		this.memory.commit(writes)
		this.recordHistory(writes)
		return Promise.resolve()
	}

	private recordHistory(writes: WriteOps<TandemTuple<DstSchema>>) {
		for (const { key, value } of writes.set ?? []) {
			if (key[0] === "record" && key[1] === "tasks")
				this.history.get(key[2])?.push(JSON.stringify(value))
		}
		for (const key of writes.remove ?? []) {
			if (key[0] === "record" && key[1] === "tasks")
				this.history.get(key[2])?.push("null")
		}
	}

	close(): Promise<void> {
		if (this.external) return this.external.close()
		this.memory.close()
		return Promise.resolve()
	}
}

/**
 * A client's view of the server in which each poke arrives as a Gatekeeper
 * event owned by that client, so the run can deliver, delay, or drop it.
 */
function remoteWithPokeEvents(
	server: Pick<DstServer, "connect" | "push" | "pull">,
	events: GatekeeperEvents,
	stats: DstStats,
): RemoteApi<DstSchema> {
	let poke: ClientApi<DstSchema>["poke"] = () => Promise.resolve()
	const pokeEvent = events.on<PokePayload<DstSchema> | undefined>(
		"poke",
		(payload) => {
			if (payload?.append) stats.appendPayloadsDelivered += 1
			// Each generated append is " t<step>", so merged text has several.
			if (payload?.append?.some(({ text }) => text.split(" t").length > 2))
				stats.mergedAppendsDelivered += 1
			return poke(payload)
		},
	)
	return {
		connect: (client) => {
			poke = client.poke
			return server.connect({
				...client,
				poke: (payload) => {
					// Payloads cross the network as JSON.
					pokeEvent.emit(payload && roundTrip(payload))
					return Promise.resolve()
				},
			})
		},
		push: (args) => server.push(args, { allowOps: true }),
		pull: (args) => server.pull(args),
	}
}

type DstClientTuple = TandemClientTuple<DstSchema>

export function roundTrip<Value>(value: Value): Value {
	return JSON.parse(JSON.stringify(value)) as Value
}

/**
 * A client's durable storage. It is its own Gatekeeper service, so it outlives
 * the client's crashes and each write is a handoff the run can order. Values
 * round-trip through JSON, as they would through a real store.
 */
class DstClientStorage implements TandemClientStorageApi<DstSchema> {
	private readonly memory = new InMemoryTupleStorage()

	scan(args?: ScanStorageArgs): Promise<DstClientTuple[]> {
		return Promise.resolve(
			roundTrip(this.memory.scan(args)) as DstClientTuple[],
		)
	}

	commit(writes: WriteOps<DstClientTuple>): Promise<void> {
		this.memory.commit(roundTrip(writes))
		return Promise.resolve()
	}

	close(): Promise<void> {
		return Promise.resolve()
	}

	clear(): Promise<void> {
		this.memory.commit({ remove: this.memory.scan().map(({ key }) => key) })
		return Promise.resolve()
	}
}

/**
 * Registered as a Gatekeeper service with only its exit gate, so a tick is a
 * held handoff to its client rather than a real timeout.
 */
class DstTimer implements TimerApi {
	waitForNextTick(): Promise<void> {
		return Promise.resolve()
	}
}

const timerGates = { gates: { enter: false, exit: true } }

/**
 * Ends the server's live poke intervals only when the run chooses a
 * `serverTick`. Not a Gatekeeper service: the server waits on it outside any
 * call. While released, as before gates activate or after the run drains,
 * intervals end at once.
 */
class DstServerTimer implements TimerApi {
	private waiting: (() => void)[] = []
	private holding = false

	get pending(): number {
		return this.waiting.length
	}

	waitForNextTick(): Promise<void> {
		if (!this.holding) return Promise.resolve()
		return new Promise((resolve) => this.waiting.push(resolve))
	}

	hold() {
		this.holding = true
	}

	/** Ends every client's current interval. */
	tick() {
		for (const resolve of this.waiting.splice(0)) resolve()
	}

	release() {
		this.holding = false
		this.tick()
	}
}

/**
 * A handoff that crosses the network: a request to the server, its reply, or a
 * poke. Timer ticks and storage writes are local, so a fault never drops them.
 */
export function isNetworkHandoff({
	sentBy,
	waitingFor,
}: Pick<PendingCall, "sentBy" | "waitingFor">): boolean {
	return sentBy === "server" || waitingFor === "server"
}

export type DstClientName = `client${number}`
export type DstTabName = `tab${number}`
export type DstWriterName = DstClientName | DstTabName
const isTab = (name: DstWriterName): name is DstTabName =>
	name.startsWith("tab")
export type { DstTabCoverage } from "./DstTabs.js"

/** Each participant's collection inventories when the run ended. */
export type DstFinalStates = Record<"server", DstInventory> &
	Record<DstClientName, DstInventory> & {
		tabs?: Record<DstTabName, DstInventory>
	}

export type DstCheckpoint = "quiescence" | "ordinary" | "repaired"

export type DstViolation =
	/** A settled client's subscribed view differs from the authoritative server. */
	| {
			kind: "subscriptionView" | "subscriptionNotification"
			checkpoint: DstCheckpoint
			client: DstWriterName
			subscriptionId: string
			spec: DstSubscriptionSpec
			expected: unknown[]
			actual: unknown[]
	  }
	/**
	 * A client showed a server-owned record older than one it showed before,
	 * or a value the server never had.
	 */
	| {
			kind: "movedBackwards"
			step: number
			client: DstClientName
			id: string
			shown: DstSchema["tasks"] | null
			previous: DstSchema["tasks"] | null
	  }
	| DstTabViolation
	| { kind: "effectRepeated"; detail: string }

/** Where a dropped handoff was lost, which decides what its sender can know. */
export type DstFaultKind =
	/** The receiver never saw the request. */
	| "requestLost"
	/** The receiver processed the request, but its reply never arrived. */
	| "responseLost"
	/** A server poke never reached its client. */
	| "pokeLost"

export class DstFaultError extends errore.createTaggedError({
	name: "DstFaultError",
	message: "$call: $kind",
}) {}

class DstCommitError extends errore.createTaggedError({
	name: "DstCommitError",
	message: "$client commit failed",
}) {}

export type DstBoundary = { call: string; sentBy: string; waitingFor: string }

type DstSubscriptionIntent =
	| {
			type: "subscribe"
			client: DstClientName
			id: string
			spec: DstSubscriptionSpec
	  }
	| { type: "unsubscribe"; client: DstClientName; id: string }

export type DstPartition = "requests" | "replies" | "both"
type DstConnectivityIntent =
	| { type: "disconnect" | "reconnect" | "heal"; client: DstClientName }
	| { type: "partition"; client: DstClientName; direction: DstPartition }

export type DstObservation = {
	spec: DstSubscriptionSpec
	latest: unknown[]
	snapshots: unknown[][]
	destroy: () => void
}

/** Gatekeeper requires an async root for subscription-triggered remote work. */
class DstClient extends TandemClient<
	DstSchema,
	typeof dstRelations,
	typeof dstMutators
> {
	appendTask(id: string, text: string, field?: "text" | "note") {
		return this.mutate.appendTask({ id, text, ...(field && { field }) })
	}
	async observe(
		install: (client: TandemClient<DstSchema, typeof dstRelations>) => void,
	): Promise<void> {
		install(this)
		await this.pullFromRemote()
	}
}

function installObservation(
	client: TandemClient<DstSchema, typeof dstRelations>,
	observations: Map<string, DstObservation>,
	id: string,
	spec: DstSubscriptionSpec,
	current: () => boolean,
	coverage: DstCoverage,
): void {
	const observation: DstObservation = {
		spec,
		latest: [],
		snapshots: [],
		destroy: () => {},
	}
	const handle = client.subscribe(subscriptionQuery(spec), (result) => {
		// A dead application cannot observe, but a broken destroy must remain visible.
		if (!current()) return
		coverage.callbacks++
		if (result.length > 0) coverage.nonemptyCallbacks++
		if (spec.kind === "tasks") {
			const identities = (rows: readonly unknown[]) =>
				new Set(
					rows.map((row) => JSON.stringify((row as { id: CollectionId }).id)),
				)
			// Initial acquisition and pure reordering do not establish a page transition.
			if (
				(observation.latest.length > 0 || observation.snapshots.length > 1) &&
				!isDeepStrictEqual(identities(observation.latest), identities(result))
			) {
				coverage.pageMembershipChanges++
			}
		}
		observation.latest = structuredClone(result)
		observation.snapshots.push(observation.latest)
	})
	observation.latest = structuredClone(handle.result)
	observation.snapshots.push(observation.latest)
	observation.destroy = handle.destroy
	observations.set(id, observation)
}

export type DstTraceRecord =
	| {
			type: "namedAppend"
			step: number
			client: DstClientName
			id: string
			text: string
			field?: "text" | "note"
			mutationId: number
	  }
	| {
			type: "mutate"
			ops: MutationOp<DstSchema>[]
			step: number
			client: DstWriterName
			mutationId: number
	  }
	| ({ type: "advance"; step: number } & DstBoundary)
	| ({
			type: "drop"
			step: number
			fault: DstFaultKind | "saveFailed"
	  } & DstBoundary)
	| { type: "serverSet"; step: number; item: DstSchema["tasks"] }
	| { type: "serverAppend"; step: number; id: string; text: string }
	| { type: "serverRemove"; step: number; id: string }
	| { type: "serverTick"; step: number }
	| { type: "crash"; step: number; client: DstWriterName }
	| { type: "restart"; step: number; client: DstClientName; clientId: string }
	| { type: "openTab"; step: number; tab: DstTabName }
	| { type: "toggleTab"; step: number; tab: DstTabName; wantsServer: boolean }
	| { type: "flushTab"; step: number; tab: DstTabName }
	| { type: "connectTab"; step: number; tab: DstTabName }
	| (DstSubscriptionIntent & { step: number })
	| (DstConnectivityIntent & { step: number })
	| { type: "checkpoint" | "repair"; step: number; outcome: DstOutcome }

function faultKind(
	{ label, waitingFor }: Pick<PendingCall, "label" | "waitingFor">,
	eventDelivered: boolean,
): DstFaultKind {
	// A poke call first stops at the poke itself; after delivery its boundaries
	// belong to the pull the poke started.
	if (label.endsWith(".poke") && !eventDelivered) return "pokeLost"
	return waitingFor === "server" ? "requestLost" : "responseLost"
}

/** Lets promise continuations settle so the next step sees stable boundaries. */
function settle(): Promise<void> {
	return new Promise((resolve) => setImmediate(resolve))
}

/** A change the run applies in one step; its trace record adds what it observed. */
export type DstIntent =
	| {
			type: "namedAppend"
			client: DstClientName
			id: string
			text: string
			field?: "text" | "note"
	  }
	/** The server writes on its own, as an agent saving its reply would. */
	| { type: "serverSet"; item: DstSchema["tasks"] }
	| { type: "serverAppend"; id: string; text: string }
	| { type: "serverRemove"; id: string }
	/** Ends the server's live poke intervals, sending merged payloads. */
	| { type: "serverTick" }
	| { type: "mutate"; client: DstWriterName; ops: MutationOp<DstSchema>[] }
	| DstSubscriptionIntent
	| DstConnectivityIntent
	| { type: "checkpoint" | "repair" }
	| { type: "advance"; call: string }
	| { type: "drop"; call: string }
	| { type: "crash"; client: DstWriterName }
	| { type: "openTab" }
	| { type: "toggleTab"; tab: DstTabName }
	| { type: "flushTab"; tab: DstTabName }
	| { type: "connectTab"; tab: DstTabName }
	| { type: "restart"; client: DstClientName }

/** Counts that show a run exercised a behavior, not just converged. */
export type DstStats = {
	namedAppendCommits: number
	effectRepeated?: string
	/** Poke payloads with appended text that reached a client. */
	appendPayloadsDelivered: number
	/** Of those, appends that merged text from several commits. */
	mergedAppendsDelivered: number
}

/** A call held at a boundary, with its stable trace name. */
export type DstPendingCall = {
	name: string
	sentBy: string
	waitingFor: string
	droppable: boolean
	handle?: CallHandle<unknown>
	label: string
}

export type DstOutcome = {
	violation: DstViolation | undefined
	states: DstFinalStates
	clientIds: readonly (string | null)[]
	settled: boolean
	coverage: DstCoverage
	tabCoverage?: DstTabCoverage
}

/**
 * The simulated system: a server, clients with timers and durable storage,
 * and the Gatekeeper harness that orders their handoffs.
 * A random run and a replay drive it through the same events.
 */
export class DstWorld implements AsyncDisposable {
	private readonly crashed = new Set<DstClientName>()
	private readonly partitions = new Map<DstClientName, DstPartition>()
	private readonly disconnected = new Set<DstClientName>()
	private readonly failedConnections = new Set<DstClientName>()
	// App intent survives a crash; handles and observations do not.
	private readonly desired: Map<DstClientName, Map<string, DstSubscriptionSpec>>
	private readonly observations: Map<DstClientName, Map<string, DstObservation>>
	// Counts restarts, so work begun for an earlier incarnation stops.
	private readonly generations: Record<DstClientName, number>
	// The generation whose storage has loaded, so it can take writes.
	private readonly loaded = new Map<DstClientName, number>()
	// Trace IDs only; correctness does not depend on tracking every mutation.
	private readonly writeCounts = new Map<DstWriterName, number>()
	private readonly callNames = new Map<CallHandle<unknown>, string>()
	private readonly labelCounts = new Map<string, number>()
	private readonly deliveredEvents = new Set<CallHandle<unknown>>()
	// Count commits through their result, not just until their first held handoff.
	private activeMutations = 0
	// Only outstanding targets, for measuring overlap; never predicts record values.
	private readonly activeTargets = new Map<
		string,
		{ client: DstClientName; keys: Set<string> }
	>()
	// Controls resolve when their call reaches its next boundary, which can
	// depend on other held calls, so a step starts them without waiting.
	private readonly inFlight: Promise<unknown>[] = []
	private held: readonly DstPendingCall[] = []
	tabs?: DstTabGroup
	// The server history index of the last value each client showed per
	// server-owned record. A restart may show older synced values, so it resets.
	private readonly shown = new Map<string, number>()
	private lastStep = -1
	private orderViolation: DstViolation | undefined
	maxPendingCalls = 0
	readonly stats: DstStats
	maxMutationsInFlight = 0

	readonly clientNames: readonly DstClientName[]
	readonly coverage: DstCoverage
	private readonly server: DstServer
	private readonly harness: DstHarness
	private readonly serverTimer: DstServerTimer
	private readonly storage: DstServerStorage

	private constructor({
		server,
		serverTimer,
		harness,
		clientNames,
		storage,
		desired,
		observations,
		coverage,
		stats,
	}: {
		serverTimer: DstServerTimer
		server: DstServer
		harness: DstHarness
		clientNames: readonly DstClientName[]
		storage: DstServerStorage
		desired: DstWorld["desired"]
		observations: DstWorld["observations"]
		coverage: DstCoverage
		stats: DstStats
	}) {
		this.serverTimer = serverTimer
		this.server = server
		this.harness = harness
		this.clientNames = clientNames
		this.storage = storage
		this.desired = desired
		this.observations = observations
		this.stats = stats
		this.coverage = coverage
		this.generations = Object.fromEntries(clientNames.map((name) => [name, 0]))
	}

	static async start({
		seed,
		clientCount = 2,
		initialSubscriptions = "full",
		tabs = 0,
		serverStorage,
	}: {
		seed: number
		clientCount?: number
		initialSubscriptions?: DstSubscriptionMode
		tabs?: number
		serverStorage?: TandemServerStorageApi<DstSchema>
	}): Promise<DstWorld> {
		const clientNames = Array.from(
			{ length: clientCount },
			(_, index): DstClientName => `client${index + 1}`,
		)
		const storage = new DstServerStorage(serverStorage)
		const serverTimer = new DstServerTimer()
		const stats: DstStats = {
			namedAppendCommits: 0,
			appendPayloadsDelivered: 0,
			mergedAppendsDelivered: 0,
		}
		const hooks = dstServerMutators(stats)
		const server = new TandemServer<
			DstSchema,
			typeof dstRelations,
			typeof dstMutators
		>({
			schema: dstSchemaDefinition,
			relations: dstRelations,
			storage,
			mutators: dstMutators,
			hooks: () => hooks,
			rng: SimPrng.idSource(seed, "server"),
			livePokeInterval: serverTimer,
		})
		const desired: DstWorld["desired"] = new Map()
		const observations: DstWorld["observations"] = new Map()
		const coverage = emptyCoverage(clientNames)
		for (const name of clientNames) {
			const specs = new Map<string, DstSubscriptionSpec>()
			if (initialSubscriptions === "full")
				for (const collection of dstCollections)
					specs.set(`inventory-${collection}`, {
						kind: "inventory",
						collection,
					})
			if (initialSubscriptions === "scoped")
				specs.set("view-0", {
					kind: "tasks",
					minPriority: 1,
					limit: 3,
					offset: 0,
					projection: false,
				})
			desired.set(name, specs)
			observations.set(name, new Map())
		}
		let world: DstWorld | undefined
		const harness = buildHarness({
			server,
			stats,
			clientNames,
			withTabs: tabs > 0,
			tabs: () => world?.tabs,
			onCreate: (name, client) => {
				const generation = world?.generations[name] ?? 0
				for (const [id, spec] of desired.get(name)!)
					installObservation(
						client,
						observations.get(name)!,
						id,
						spec,
						() =>
							!world?.crashed.has(name) &&
							(world?.generations[name] ?? 0) === generation,
						coverage,
					)
			},
			idSource: (label) =>
				SimPrng.idSource(seed, `${label}.${world?.generations[label] ?? 0}`),
		})
		world = new DstWorld({
			server,
			serverTimer,
			storage,
			harness,
			clientNames,
			desired,
			observations,
			coverage,
			stats,
		})
		for (const name of clientNames) await world.boot(name)
		if (tabs > 0) {
			const group = new DstTabGroup(seed, (tab) => ({
				connect: async (api) =>
					(await harness.tabsRemote.connect(tab, api)).result,
				push: async (args) => (await harness.tabsRemote.push(args)).result,
				pull: async (args) => (await harness.tabsRemote.pull(args)).result,
			}))
			world.tabs = group
			for (let index = 0; index < tabs; index++) group.open()
			await group.heal(() => Promise.resolve(), settle)
		}
		await harness.activateGates()
		serverTimer.hold()
		return world
	}

	get running(): readonly DstClientName[] {
		return this.clientNames.filter((name) => !this.crashed.has(name))
	}

	get down(): readonly DstClientName[] {
		return this.clientNames.filter((name) => this.crashed.has(name))
	}

	get crashable(): readonly DstWriterName[] {
		return [...this.running, ...(this.tabs?.alive ?? [])]
	}

	get mutationsInFlight(): number {
		return this.activeMutations + (this.tabs?.pendingCommits ?? 0)
	}

	/** Running clients whose storage has loaded, so they can take writes. */
	get writers(): readonly DstWriterName[] {
		return [
			...this.running.filter(
				(name) => this.loaded.get(name) === this.generations[name],
			),
			...(this.tabs?.writers ?? []),
		]
	}

	/** Calls held at a boundary, named in creation order so names never depend on choices. */
	pending(): readonly DstPendingCall[] {
		this.held = [
			...this.harness.pendingCalls().map((call) => ({
				name: this.nameCall(call),
				sentBy: call.sentBy,
				waitingFor: call.waitingFor,
				droppable:
					isNetworkHandoff(call) &&
					!call.label.startsWith("tabsRemote.connect"),
				handle: call.handle,
				label: call.label,
			})),
			...(this.tabs?.pending() ?? []).map(({ name, kind, droppable }) => ({
				name,
				label: name,
				sentBy: "tabs",
				waitingFor: kind,
				droppable,
			})),
		]
		this.maxPendingCalls = Math.max(this.maxPendingCalls, this.held.length)
		return this.held
	}

	/** Applies one event and returns its trace record. */
	async apply(step: number, intent: DstIntent): Promise<DstTraceRecord> {
		this.lastStep = step
		switch (intent.type) {
			case "checkpoint":
				return {
					type: "checkpoint",
					step,
					outcome: await this.checkpoint("ordinary"),
				}
			case "repair":
				return { type: "repair", step, outcome: await this.finish("repaired") }
			case "partition":
				this.partitions.set(intent.client, intent.direction)
				return { ...intent, step }
			case "heal":
				this.partitions.delete(intent.client)
				return { ...intent, step }
			case "disconnect":
				this.coverage.disconnects++
				this.disconnected.add(intent.client)
				this.trackConnection(intent.client, "disconnect")
				return { ...intent, step }
			case "reconnect":
				this.coverage.reconnects++
				this.disconnected.delete(intent.client)
				this.failedConnections.delete(intent.client)
				this.trackConnection(intent.client, "connect")
				return { ...intent, step }
			case "mutate":
				return this.write(step, intent)
			case "namedAppend":
				return this.writeNamedAppend(step, intent)
			case "openTab":
				return { type: "openTab", step, tab: this.tabs!.open() }
			case "toggleTab":
				return {
					type: "toggleTab",
					step,
					tab: intent.tab,
					wantsServer: this.tabs!.toggle(intent.tab),
				}
			case "flushTab":
				this.tabs!.flush(intent.tab)
				return { ...intent, step }
			case "connectTab":
				this.tabs!.connect(intent.tab)
				return { ...intent, step }
			case "serverSet":
			case "serverAppend":
			case "serverRemove":
				return this.writeOnServer(step, intent)
			case "serverTick":
				this.serverTimer.tick()
				return { type: "serverTick", step }
			case "subscribe":
				this.coverage.subscriptionChanges++
				this.unsubscribe(intent.client, intent.id)
				this.desired.get(intent.client)!.set(intent.id, intent.spec)
				this.installSubscription(intent.client, intent.id, intent.spec)
				return { ...intent, step }
			case "unsubscribe":
				this.coverage.subscriptionChanges++
				this.unsubscribe(intent.client, intent.id)
				return { ...intent, step }
			case "advance": {
				const target = this.find(intent.call)
				if (!target.handle) {
					this.tabs!.advance(target.name)
					return { type: "advance", step, ...boundaryOf(target) }
				}
				if (this.partitionBlocks(target))
					return this.apply(step, { type: "drop", call: intent.call })
				this.deliveredEvents.add(target.handle)
				this.inFlight.push(target.handle.continueTo(target.waitingFor))
				return { type: "advance", step, ...boundaryOf(target) }
			}
			case "drop": {
				const target = this.find(intent.call)
				if (!target.handle) {
					const kind =
						this.tabs!.drop(target.name) === "poke" ? "pokeLost" : "saveFailed"
					if (kind === "pokeLost") this.coverage.dropsByKind.pokeLost++
					return { type: "drop", step, fault: kind, ...boundaryOf(target) }
				}
				const kind = faultKind(
					{ label: target.label!, waitingFor: target.waitingFor },
					this.deliveredEvents.has(target.handle),
				)
				this.coverage.dropsByKind[kind]++
				if (kind === "responseLost" && target.label === "tabsRemote.push")
					this.tabs!.coverage.acceptedPushRepliesLost++
				if (this.partitionBlocks(target)) this.coverage.partitionDrops++
				this.inFlight.push(
					target.handle.fail(new DstFaultError({ call: target.name, kind })),
				)
				return { type: "drop", step, fault: kind, ...boundaryOf(target) }
			}
			case "crash": {
				if (isTab(intent.client)) {
					this.tabs!.crash(intent.client)
					this.writeCounts.delete(intent.client)
					return { type: "crash", step, client: intent.client }
				}
				const calls = this.harness
					.pendingCalls()
					.filter(({ label }) => label === `${intent.client}.commit`)
				if (
					calls.some(
						({ waitingFor }) => waitingFor === `${intent.client}Storage`,
					)
				)
					this.coverage.crashesBeforeDurability++
				if (calls.some(({ sentBy }) => sentBy === `${intent.client}Storage`))
					this.coverage.crashesAfterDurability++
				this.crashed.add(intent.client)
				for (const id of serverOwnedIds)
					this.shown.delete(`${intent.client}:${id}`)
				this.observations.get(intent.client)!.clear()
				this.loaded.delete(intent.client)
				this.disconnected.delete(intent.client)
				this.failedConnections.delete(intent.client)
				this.writeCounts.delete(intent.client)
				await this.harness.crash(intent.client)
				return { type: "crash", step, client: intent.client }
			}
			case "restart": {
				this.coverage.restarts++
				this.crashed.delete(intent.client)
				this.generations[intent.client] += 1
				await this.harness.restart(intent.client)
				this.inFlight.push(this.boot(intent.client))
				return {
					type: "restart",
					step,
					client: intent.client,
					clientId: this.harness[intent.client].clientId,
				}
			}
		}
	}

	/** Server poke intervals waiting for a `serverTick`. */
	get serverTicksPending(): number {
		return this.serverTimer.pending
	}

	hasPending(call: string): boolean {
		return this.held.some(({ name }) => name === call)
	}

	/** Let continuations reach their next boundary before choosing another event. */
	async settle(): Promise<void> {
		await settle()
		this.observeOrder()
	}

	/** Each client's server-owned records only move forward through server history. */
	private observeOrder() {
		for (const name of this.clientNames) {
			// Scoped readers may legitimately evict records when their query changes.
			const fullTasks = [...this.subscriptions(name).values()].some(
				({ spec }) => spec.kind === "inventory" && spec.collection === "tasks",
			)
			if (this.loaded.get(name) !== this.generations[name] || !fullTasks) {
				for (const id of serverOwnedIds) this.shown.delete(`${name}:${id}`)
				continue
			}
			const tasks = this.inventoryOf(name).tasks
			for (const id of serverOwnedIds) {
				const history = this.storage.history.get(id)!
				const previous = this.shown.get(`${name}:${id}`) ?? 0
				const task = tasks.find((candidate) => candidate.id === id)
				const index = history.indexOf(JSON.stringify(task ?? null), previous)
				if (index !== -1) {
					this.shown.set(`${name}:${id}`, index)
					continue
				}
				this.orderViolation ??= {
					kind: "movedBackwards",
					step: this.lastStep,
					client: name,
					id,
					shown: task ?? null,
					previous: JSON.parse(history[previous]!) as DstSchema["tasks"] | null,
				}
			}
		}
	}

	/**
	 * Stop injecting faults, finish retained writes over healthy connections,
	 * then pull every client and compare its subscribed view with the server.
	 */
	async finish(checkpoint: DstCheckpoint = "quiescence"): Promise<DstOutcome> {
		await this.drain()

		for (const name of this.down) {
			this.crashed.delete(name)
			this.generations[name] += 1
			await this.harness.restart(name)
			await this.boot(name)
		}
		await this.tabs?.heal(async (client) => {
			for (let attempt = 0; attempt < 3; attempt++)
				if (
					await client.flushClientStorage().then(
						() => true,
						() => false,
					)
				)
					break
			await client.connect()
		}, settle)
		for (const name of this.clientNames) {
			this.disconnected.delete(name)
			this.failedConnections.delete(name)
			await (
				await this.harness[name].disconnect()
			).result
			await (
				await this.harness[name].connect()
			).result
		}
		await this.drain()
		for (const name of this.clientNames) {
			await (
				await this.harness[name].pullFromRemote()
			).result
		}
		await this.drain()
		await this.tabs?.heal((client) => client.pullFromRemote(), settle)
		this.tabs?.checkDelivered()

		const outcome = await this.checkpoint(checkpoint)
		for (const name of this.running) {
			await (
				await this.harness[name].disconnect()
			).result
		}
		return outcome
	}

	async checkpoint(checkpoint: DstCheckpoint): Promise<DstOutcome> {
		this.observeOrder()
		const states: DstFinalStates = {
			server: await this.serverInventory(),
			...Object.fromEntries(
				this.clientNames.map((name) => [name, this.inventoryOf(name)]),
			),
			...(this.tabs && {
				tabs: Object.fromEntries(
					this.tabs.alive.map((tab) => [tab, this.tabs!.inventoryOf(tab)]),
				),
			}),
		}
		const violations: DstViolation[] = []
		for (const client of this.running) {
			for (const [subscriptionId, observation] of this.subscriptions(client)) {
				const { spec, latest } = observation
				const expected = expectedView(states.server, spec)
				const local = this.harness[client].query(subscriptionQuery(spec))
				this.coverage.viewChecks++
				if (expected.length > 0) {
					this.coverage.nonemptyViewChecks++
					if (spec.kind === "tasks" && spec.offset > 0)
						this.coverage.offsetViewChecks++
					if (spec.kind === "inventory" && spec.collection === "memberships")
						this.coverage.compoundViewChecks++
					if (
						spec.kind === "projectTasks" &&
						(expected as { tasks: { comments: unknown[] }[] }[]).some(
							({ tasks }) => tasks.some(({ comments }) => comments.length > 0),
						)
					)
						this.coverage.nestedRelationChecks++
					if (
						spec.kind === "taskComments" &&
						(expected as { comments: unknown[] }[]).some(
							({ comments }) => comments.length > 0,
						)
					)
						this.coverage.nestedRelationChecks++
				}
				if (!isDeepStrictEqual(local, expected))
					violations.push({
						kind: "subscriptionView",
						checkpoint,
						client,
						subscriptionId,
						spec,
						expected,
						actual: structuredClone(local),
					})
				if (!isDeepStrictEqual(latest, expected))
					violations.push({
						kind: "subscriptionNotification",
						checkpoint,
						client,
						subscriptionId,
						spec,
						expected,
						actual: latest,
					})
			}
		}
		if (this.tabs)
			for (const tab of this.tabs.alive) {
				const actual = this.tabs.inventoryOf(tab)
				for (const collection of dstCollections) {
					this.coverage.viewChecks++
					if (!isDeepStrictEqual(actual[collection], states.server[collection]))
						violations.push({
							kind: "subscriptionView",
							checkpoint,
							client: tab,
							subscriptionId: `inventory-${collection}`,
							spec: { kind: "inventory", collection },
							expected: states.server[collection],
							actual: actual[collection],
						})
				}
			}
		const clientIds = this.clientNames.map((name) =>
			this.crashed.has(name) ? null : this.harness[name].clientId,
		)
		return {
			violation: this.stats.effectRepeated
				? { kind: "effectRepeated", detail: this.stats.effectRepeated }
				: (this.orderViolation ?? violations[0] ?? this.tabs?.violation),
			states,
			clientIds,
			coverage: structuredClone(this.coverage),
			...(this.tabs && { tabCoverage: structuredClone(this.tabs.coverage) }),
			settled:
				this.mutationsInFlight === 0 &&
				this.clientNames.every((name) => this.writers.includes(name)) &&
				(this.tabs?.writers.length ?? 0) === (this.tabs?.alive.length ?? 0) &&
				this.offline.length === 0 &&
				this.harness.pendingCalls().length === 0 &&
				this.serverTicksPending === 0 &&
				(this.tabs?.pending().length ?? 0) === 0,
		}
	}

	/** Releases every gate and lets all work in flight finish. */
	async drain(): Promise<void> {
		// Merged payloads go out before the gates release, so their pokes run.
		this.serverTimer.release()
		await settle()
		await this.harness.deactivateGatesAndSettle()
		const results = await Promise.all(this.inFlight)
		const error = results.find((result) => result instanceof Error)
		if (error instanceof Error) throw error
	}

	async [Symbol.asyncDispose](): Promise<void> {
		this.tabs?.close()
		await this.harness[Symbol.asyncDispose]()
		await this.server.close()
	}

	private write(
		step: number,
		intent: Extract<DstIntent, { type: "mutate" }>,
	): DstTraceRecord {
		const mutationId = (this.writeCounts.get(intent.client) ?? 0) + 1
		this.writeCounts.set(intent.client, mutationId)
		if (isTab(intent.client)) {
			this.tabs!.commit(
				intent.client,
				intent.ops,
				`tab-write-${intent.client}-${mutationId}`,
			)
			this.maxMutationsInFlight = Math.max(
				this.maxMutationsInFlight,
				this.mutationsInFlight,
			)
			return { ...intent, step, mutationId }
		}
		const clientName = intent.client as DstClientName
		const client = this.harness[clientName]
		const tx = client.transact()
		const effectiveOps: MutationOp<DstSchema>[] = []
		for (const op of intent.ops) {
			switch (op.type) {
				case "set":
					tx.set(op.collection, op.value)
					effectiveOps.push(op)
					break
				case "update":
					// Read the public transaction, including earlier operations in this batch.
					if (
						Object.keys(op.payload).length > 0 &&
						tx.get(op.collection, op.id) !== undefined
					)
						effectiveOps.push(op)
					tx.update(op.collection, op.id, op.payload)
					break
				case "remove":
					tx.remove(op.collection, op.id)
					effectiveOps.push(op)
					break
				default:
					return unreachable(op)
			}
		}
		let filterMembershipChanges = 0
		const taskIds = new Set(
			effectiveOps
				.filter((op) => op.collection === "tasks")
				.map((op) =>
					matchBy(op, "type", {
						set: ({ value }) => value.id,
						update: ({ id }) => id,
						remove: ({ id }) => id,
					}),
				),
		)
		for (const id of taskIds) {
			const before = client.query({ collection: "tasks", where: { id } })[0]
			const after = tx.get("tasks", id)
			if (!before || !after) continue
			for (const observer of this.running) {
				for (const { spec } of this.subscriptions(observer).values()) {
					if (
						spec.kind === "tasks" &&
						matchesTask(before, spec) !== matchesTask(after, spec)
					)
						filterMembershipChanges++
				}
			}
		}
		const generation = this.generations[intent.client]
		const token = `${intent.client}.${generation}.${mutationId}`
		const keys = new Set(
			effectiveOps.map((op) =>
				JSON.stringify([
					op.collection,
					matchBy(op, "type", {
						set: ({ value }) => value.id,
						update: ({ id }) => id,
						remove: ({ id }) => id,
					}),
				]),
			),
		)
		if (
			[...this.activeTargets.values()].some(
				(active) =>
					active.client !== intent.client &&
					[...keys].some((key) => active.keys.has(key)),
			)
		)
			this.coverage.conflictingMutations++
		this.activeTargets.set(token, { client: intent.client, keys })
		this.activeMutations += 1
		this.maxMutationsInFlight = Math.max(
			this.maxMutationsInFlight,
			this.mutationsInFlight,
		)
		// The handle arrives only once the commit reaches a boundary, which may
		// wait on another held call. Its call shows up in pending() then.
		this.inFlight.push(
			client.commit(tx).then(async (handle) => {
				const result = await handle.result.catch(
					(cause: unknown) =>
						new DstCommitError({ client: intent.client, cause }),
				)
				this.activeMutations -= 1
				this.activeTargets.delete(token)
				if (result instanceof Error) {
					// Faults and terminated incarnations are already recorded in the trace.
					if (
						result.cause instanceof DstFaultError ||
						(result.cause instanceof GatekeeperError &&
							(this.crashed.has(clientName) ||
								this.generations[clientName] !== generation))
					)
						return
					return result
				}
				if (effectiveOps.length === 0) return
				this.coverage.filterMembershipChanges += filterMembershipChanges
				this.coverage.mutationsByClient[clientName]!++
				const size = String(effectiveOps.length)
				this.coverage.batchSizes[size] =
					(this.coverage.batchSizes[size] ?? 0) + 1
				if (effectiveOps.length > 1) this.coverage.multiOperationCommits++
				if (effectiveOps.some((op) => op.collection === "memberships"))
					this.coverage.compoundIdentityCommits++
				for (const op of effectiveOps) {
					const key = `${op.collection}.${op.type}`
					this.coverage.operations[key] =
						(this.coverage.operations[key] ?? 0) + 1
				}
			}),
		)
		return { ...intent, step, mutationId }
	}

	private writeNamedAppend(
		step: number,
		intent: Extract<DstIntent, { type: "namedAppend" }>,
	): DstTraceRecord {
		const mutationId = (this.writeCounts.get(intent.client) ?? 0) + 1
		this.writeCounts.set(intent.client, mutationId)
		const generation = this.generations[intent.client]
		this.activeMutations += 1
		this.inFlight.push(
			this.harness[intent.client]
				.appendTask(intent.id, intent.text, intent.field)
				.then(async (handle) => {
					const result = await handle.result.catch(
						(cause: unknown) =>
							new DstCommitError({ client: intent.client, cause }),
					)
					this.activeMutations -= 1
					if (result instanceof Error) {
						if (
							this.crashed.has(intent.client) ||
							this.generations[intent.client] !== generation
						)
							return
						if (result.cause instanceof DstFaultError) return
						// Concurrent removal can make an optimistic append a server refusal.
						if (result.cause instanceof MutationRejectedError) return
						return result
					}
					this.coverage.mutationsByClient[intent.client]!++
				}),
		)
		return { ...intent, step, mutationId }
	}

	private writeOnServer(
		step: number,
		intent: Extract<
			DstIntent,
			{ type: "serverSet" | "serverAppend" | "serverRemove" }
		>,
	): DstTraceRecord {
		const tx = this.harness.server.transact()
		switch (intent.type) {
			case "serverSet":
				tx.set("tasks", intent.item)
				break
			case "serverAppend":
				break
			case "serverRemove":
				tx.remove("tasks", intent.id)
				break
			default:
				return unreachable(intent)
		}
		// Its pokes, with the records each client views, are events the run can lose.
		this.activeMutations += 1
		this.maxMutationsInFlight = Math.max(
			this.maxMutationsInFlight,
			this.mutationsInFlight,
		)
		const committed =
			intent.type === "serverAppend"
				? this.harness.server.appendTask({
						id: intent.id,
						text: intent.text,
					})
				: this.harness.server.commit(tx)
		this.inFlight.push(
			committed.then(async (handle) => {
				await handle.result
				this.activeMutations -= 1
			}),
		)
		return { ...intent, step }
	}

	private async boot(name: DstClientName): Promise<void> {
		const generation = this.generations[name]
		const current = () =>
			!this.crashed.has(name) && this.generations[name] === generation
		await this.harness[name].ready
		if (!current()) return
		this.loaded.set(name, generation)
		const connect = await this.harness[name].connect()
		await connect.result.catch((error: unknown) => {
			if (!current()) return
			// A dropped connect or first pull leaves the client offline, as a
			// real network would, until it reconnects at quiescence.
			if (!(error instanceof DstFaultError)) throw error
			this.failedConnections.add(name)
			// All clients reconnect over a healthy transport during finish().
		})
	}

	get partitioned(): readonly DstClientName[] {
		return [...this.partitions.keys()]
	}

	get offline(): readonly DstClientName[] {
		return [
			...new Set([...this.disconnected, ...this.failedConnections]),
		].filter((name) => !this.crashed.has(name))
	}

	private partitionBlocks(call: DstPendingCall): boolean {
		if (!isNetworkHandoff(call)) return false
		const client = call.sentBy === "server" ? call.waitingFor : call.sentBy
		const direction = this.partitions.get(client as DstClientName)
		return (
			direction === "both" ||
			(direction === "requests" && call.waitingFor === "server") ||
			(direction === "replies" && call.sentBy === "server")
		)
	}

	private trackConnection(
		name: DstClientName,
		method: "connect" | "disconnect",
	): void {
		const generation = this.generations[name]
		this.inFlight.push(
			this.harness[name][method]().then(async (handle) => {
				await handle.result.catch((error: unknown) => {
					if (this.crashed.has(name) || this.generations[name] !== generation)
						return
					if (error instanceof DstFaultError) {
						this.failedConnections.add(name)
						return
					}
					throw error
				})
			}),
		)
	}

	subscriptions(name: DstClientName): ReadonlyMap<string, DstObservation> {
		return this.observations.get(name)!
	}

	private unsubscribe(name: DstClientName, id: string): void {
		this.observations.get(name)!.get(id)?.destroy()
		this.observations.get(name)!.delete(id)
		this.desired.get(name)!.delete(id)
	}

	private installSubscription(
		name: DstClientName,
		id: string,
		spec: DstSubscriptionSpec,
	): void {
		const generation = this.generations[name]
		this.inFlight.push(
			this.harness[name]
				.observe((client) =>
					installObservation(
						client,
						this.observations.get(name)!,
						id,
						spec,
						() =>
							!this.crashed.has(name) && this.generations[name] === generation,
						this.coverage,
					),
				)
				.then(async (handle) => {
					await handle.result.catch((error: unknown) => {
						if (
							error instanceof DstFaultError ||
							this.crashed.has(name) ||
							this.generations[name] !== generation
						)
							return
						throw error
					})
				}),
		)
	}

	private nameCall(pending: PendingCall): string {
		const existing = this.callNames.get(pending.handle)
		if (existing) return existing
		const count = (this.labelCounts.get(pending.label) ?? 0) + 1
		this.labelCounts.set(pending.label, count)
		const name = `${pending.label}#${count}`
		this.callNames.set(pending.handle, name)
		return name
	}

	private find(call: string): DstPendingCall {
		const target = this.held.find(({ name }) => name === call)
		if (!target) throw new Error(`${call} is not held at a boundary`)
		return target
	}

	query<Query extends RelationalQuery<DstSchema, typeof dstRelations>>({
		client,
		query,
	}: {
		client: DstClientName
		query: Query
	}) {
		return this.harness[client].query(query)
	}

	inventoryOf(name: DstClientName): DstInventory {
		const client = this.harness[name]
		return {
			projects: [...client.query({ collection: "projects" })].sort(byId),
			tasks: [...client.query({ collection: "tasks" })].sort(byId),
			comments: [...client.query({ collection: "comments" })].sort(byId),
			memberships: [...client.query({ collection: "memberships" })].sort(byId),
		}
	}

	private async serverInventory(): Promise<DstInventory> {
		const tx = this.server.transact()
		await using cleanup = new errore.AsyncDisposableStack()
		cleanup.defer(() => tx.cancel())
		const [projects, tasks, comments, memberships] = await Promise.all([
			tx.list("projects"),
			tx.list("tasks"),
			tx.list("comments"),
			tx.list("memberships"),
		])
		return {
			projects: [...projects].sort(byId),
			tasks: [...tasks].sort(byId),
			comments: [...comments].sort(byId),
			memberships: [...memberships].sort(byId),
		}
	}
}

function byId(a: { id: CollectionId }, b: { id: CollectionId }): number {
	return JSON.stringify(a.id).localeCompare(JSON.stringify(b.id))
}

function boundaryOf(call: DstPendingCall): DstBoundary {
	return { call: call.name, sentBy: call.sentBy, waitingFor: call.waitingFor }
}

function buildHarness({
	server,
	stats,
	clientNames,
	idSource,
	onCreate,
	withTabs,
	tabs,
}: {
	server: DstServer
	clientNames: readonly DstClientName[]
	stats: DstStats
	idSource: (label: DstClientName) => RngApi
	onCreate: (name: DstClientName, client: DstClient) => void
	withTabs: boolean
	tabs: () => DstTabGroup | undefined
}) {
	const logger = new Logger({ sinks: [] })
	const createClient = (
		label: DstClientName,
		remote: RemoteApi<DstSchema>,
		timer: TimerApi,
		storage: TandemClientStorageApi<DstSchema>,
	) => {
		const client = new DstClient({
			remote,
			clientStorage: storage,
			schema: dstSchemaDefinition,
			relations: dstRelations,
			mutators: dstMutators,
			logger,
			rng: idSource(label),
			autoConnect: false,
			syncInterval: timer,
			clientStorageWriteInterval: timer,
			// Each tick of the client's timer is one quiet check, and the
			// maximum delay is a single tick of its own.
			livePullDelay: timer,
			livePullMaxDelay: timer,
		})
		onCreate(label, client)
		return client
	}

	let builder = new Gatekeeper().add("server", () =>
		observedServer(server, tabs, withTabs),
	)
	// Register dependencies before any client, preserving stable construction order.
	for (const name of clientNames) {
		builder = builder.add(`${name}Timer`, () => new DstTimer(), timerGates)
	}
	for (const name of clientNames) {
		builder = builder.add(`${name}Storage`, () => new DstClientStorage())
	}
	for (const name of clientNames) {
		builder = builder.add(name, (services, { events }) => {
			// Gatekeeper's fluent type cannot accumulate runtime-generated keys.
			const dependencies = services as typeof services &
				Record<`${DstClientName}Timer`, TimerApi> &
				Record<`${DstClientName}Storage`, TandemClientStorageApi<DstSchema>>
			return createClient(
				name,
				remoteWithPokeEvents(services.server, events, stats),
				dependencies[`${name}Timer`],
				dependencies[`${name}Storage`],
			)
		})
	}
	return (
		withTabs
			? builder
					.add("tabsRemote", ({ server }) => tabsRemote(server, tabs))
					.build()
			: builder.build()
	) as DstHarness
}

type DstHarness = Harness<
	{
		server: ReturnType<typeof observedServer>
		tabsRemote: ReturnType<typeof tabsRemote>
	} & Record<DstClientName, DstClient>
>

/** Tab leader calls cross the same network gate as standalone clients. */
function tabsRemote(
	server: Pick<DstServer, "connect" | "push" | "pull">,
	tabs: () => DstTabGroup | undefined,
) {
	return {
		connect: async (tab: DstTabName, api: ClientApi<DstSchema>) => {
			const connected = await server.connect({
				...api,
				poke: (payload) => {
					tabs()?.poke(tab, () => api.poke(payload && roundTrip(payload)))
					return Promise.resolve()
				},
			})
			if (typeof connected === "function") {
				if (!tabs()?.isAlive(tab)) await connected()
				else {
					tabs()?.connected()
					return tabs()!.trackRemoteDisconnect(tab, connected)
				}
			}
			return connected
		},
		push: (args: Parameters<RemoteApi<DstSchema>["push"]>[0]) =>
			server.push(args, { allowOps: true }),
		pull: (args: Parameters<RemoteApi<DstSchema>["pull"]>[0]) =>
			server.pull(args),
	}
}

function observedServer(
	server: DstServer,
	tabs: () => DstTabGroup | undefined,
	withTabs: boolean,
) {
	return {
		transact: () => server.transact(),
		commit: (tx: Parameters<typeof server.commit>[0]) => server.commit(tx),
		// Gatekeeper wraps top-level methods; invoke the server's public named method inside it.
		appendTask: (args: { id: string; text: string }) =>
			server.mutate.appendTask(args),
		connect: (api: Parameters<typeof server.connect>[0]) => server.connect(api),
		pull: (args: Parameters<typeof server.pull>[0]) => server.pull(args),
		push: withTabs
			? async (
					args: Parameters<typeof server.push>[0],
					options?: Parameters<typeof server.push>[1],
				) => {
					const response = await server.push(args, options)
					if (!("error" in response)) tabs()?.accepted(args)
					return response
				}
			: server.push,
	}
}
