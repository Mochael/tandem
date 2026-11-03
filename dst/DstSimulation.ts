import type { MutationOp } from "@tanishqkancharla/tandem-core"
import type { TandemServerStorageApi } from "@tanishqkancharla/tandem-server"
import type { DstCoverage } from "./DstCoverage.js"
import type { DstArtifactSink } from "./DstReplay.js"
import { recover, type DstRecoveryResult } from "./DstRecovery.js"
import {
	type DstClientName,
	type DstWriterName,
	type DstTabCoverage,
	type DstTabName,
	type DstFinalStates,
	type DstStats,
	type DstIntent,
	type DstPendingCall,
	type DstSchema,
	type DstTraceRecord,
	type DstViolation,
	DstWorld,
	dstCollections,
	serverOwnedIds,
} from "./DstWorld.js"
import { SimPrng } from "./SimPrng.js"
import type {
	DstSubscriptionMode,
	DstSubscriptionSpec,
} from "./DstSubscriptions.js"

export type {
	DstClientName,
	DstFaultKind,
	DstFinalStates,
	DstInventory,
	DstSchema,
	DstTraceRecord,
	DstViolation,
	DstTabCoverage,
} from "./DstWorld.js"
export { DstFaultError, dstRelations, dstSchemaDefinition } from "./DstWorld.js"

export interface DstRunOptions {
	seed: number
	steps: number
	faultRate?: number
	/** Chance per step that a running client crashes. */
	crashRate?: number
	/** Tabs sharing one durable replica, in addition to the standalone clients. */
	tabs?: number
	clientCount?: number
	/** IDs per collection's shared/owned pool; member IDs for memberships. */
	recordCount?: number
	maxMutationsInFlight?: number
	/** Chance of targeting the shared pool rather than the writer's own pool. */
	sharedWriteRate?: number
	/** Chance a writer calls the shared named append mutation. */
	namedAppendRate?: number
	initialSubscriptions?: DstSubscriptionMode
	subscriptionRate?: number
	connectivityRate?: number
	recoveryEvents?: number
}

export function resolveRunOptions(
	options: DstRunOptions,
): Required<DstRunOptions> {
	return {
		seed: options.seed,
		steps: options.steps,
		faultRate: options.faultRate ?? 0,
		crashRate: options.crashRate ?? 0,
		tabs: options.tabs ?? 0,
		clientCount: options.clientCount ?? 2,
		recordCount: options.recordCount ?? 3,
		maxMutationsInFlight: options.maxMutationsInFlight ?? 4,
		sharedWriteRate: options.sharedWriteRate ?? 1,
		namedAppendRate: options.namedAppendRate ?? 0,
		initialSubscriptions: options.initialSubscriptions ?? "full",
		subscriptionRate: options.subscriptionRate ?? 0.08,
		connectivityRate: options.connectivityRate ?? 0.02,
		recoveryEvents: options.recoveryEvents ?? (options.clientCount ?? 2) * 500,
	}
}

export type DstRunResult = {
	seed: number
	stepsCompleted: number
	trace: readonly DstTraceRecord[]
	clientIds: readonly (string | null)[]
	states: DstFinalStates
	violation: DstViolation | undefined
	maxPendingCalls: number
	stats: DstStats
	maxMutationsInFlight: number
	recovery: DstRecoveryResult
	coverage: DstCoverage
	tabCoverage?: DstTabCoverage
}

/** How often a step starts a mutation rather than advancing a pending call. */
const mutateRate = 0.35
/** Share of new writes the server makes itself rather than a client. */
const serverWriteRate = 0.4
/** Chance per step that a crashed client restarts. */
const restartRate = 0.25
/** Chance per step that waiting server poke intervals end. */
const serverTickRate = 0.05
const toggleTabRate = 0.05

/** A random run: each step applies one event chosen from what is possible now. */
export class DstSimulation {
	private readonly options: Required<DstRunOptions>

	constructor(options: DstRunOptions) {
		this.options = resolveRunOptions(options)
	}

	async execute(
		sink?: DstArtifactSink,
		serverStorage?: TandemServerStorageApi<DstSchema>,
	): Promise<DstRunResult> {
		const { seed, steps } = this.options
		sink?.write({ kind: "header", format: 1, options: this.options })
		await using world = await DstWorld.start({ ...this.options, serverStorage })
		// Drives choices only; ids come from their own streams.
		const rng = new SimPrng(seed)
		const trace: DstTraceRecord[] = []

		const stepsCompleted = steps
		for (let step = 0; step < steps; step++) {
			const intent = this.choose(rng, world, world.pending(), step)
			if (intent) {
				const record = await world.apply(step, intent)
				trace.push(record)
				sink?.write({ kind: "event", ...record })
			}
			await world.settle()
		}

		const outcome = await recover(world, {
			startStep: steps,
			eventBudget: this.options.recoveryEvents,
			record: (event) => {
				trace.push(event)
				sink?.write({ kind: "event", ...event })
			},
		})
		sink?.write({ kind: "outcome", stepsCompleted, ...outcome })
		return {
			seed,
			stepsCompleted,
			trace,
			...outcome,
			maxPendingCalls: world.maxPendingCalls,
			stats: world.stats,
			maxMutationsInFlight: world.maxMutationsInFlight,
		}
	}

	private choose(
		rng: SimPrng,
		world: DstWorld,
		pending: readonly DstPendingCall[],
		step: number,
	): DstIntent | undefined {
		const { faultRate, crashRate, maxMutationsInFlight } = this.options
		// Faults can lose network messages or fail tab leader saves, but not timers.
		const droppable = pending.filter(({ droppable }) => droppable)
		const { down, crashable, writers } = world
		const standaloneWriters = writers.filter(
			(writer): writer is DstClientName => !writer.startsWith("tab"),
		)

		// Without crashes these draw nothing, so crash-free runs are unchanged.
		if (down.length > 0 && rng.boolean(restartRate)) {
			return { type: "restart", client: rng.pick(down) }
		}
		if (
			this.options.tabs > 0 &&
			(world.tabs?.alive.length ?? 0) < this.options.tabs &&
			rng.boolean(restartRate)
		)
			return { type: "openTab" }
		if (crashRate > 0 && crashable.length > 0 && rng.boolean(crashRate)) {
			return { type: "crash", client: rng.pick(crashable) }
		}
		if (faultRate > 0 && droppable.length > 0 && rng.boolean(faultRate)) {
			return { type: "drop", call: rng.pick(droppable).name }
		}
		if (world.serverTicksPending > 0 && rng.boolean(serverTickRate)) {
			return { type: "serverTick" }
		}
		if (
			writers.length > 0 &&
			this.options.connectivityRate > 0 &&
			rng.boolean(this.options.connectivityRate)
		) {
			const client = rng.pick(writers)
			if (client.startsWith("tab"))
				return { type: "toggleTab", tab: client as DstTabName }
			const standalone = client as DstClientName
			if (world.partitioned.includes(standalone) && rng.boolean(0.5))
				return { type: "heal", client: standalone }
			if (world.offline.includes(standalone))
				return { type: "reconnect", client: standalone }
			if (rng.boolean(0.3)) return { type: "disconnect", client: standalone }
			return {
				type: "partition",
				client: standalone,
				direction: rng.pick(["requests", "replies", "both"]),
			}
		}
		if (
			standaloneWriters.length > 0 &&
			this.options.subscriptionRate > 0 &&
			rng.boolean(this.options.subscriptionRate)
		) {
			const client = rng.pick(standaloneWriters)
			const id = `view-${rng.int(0, 2)}`
			if (world.subscriptions(client).has(id) && rng.boolean(0.3))
				return { type: "unsubscribe", client, id }
			const inventory = world.inventoryOf(client)
			const projectId =
				inventory.projects.length > 0
					? rng.pick(inventory.projects).id
					: `item-${rng.int(1, this.options.recordCount)}`
			const taskId =
				inventory.tasks.length > 0
					? rng.pick(inventory.tasks).id
					: `item-${rng.int(1, this.options.recordCount)}`
			const choices: DstSubscriptionSpec[] = [
				{
					kind: "tasks",
					...(rng.boolean(0.5) ? { projectId } : {}),
					...(rng.boolean(0.7) ? { done: rng.boolean(0.5) } : {}),
					minPriority: rng.int(1, 4),
					limit: rng.int(1, 4),
					offset: rng.int(0, 2),
					projection: rng.boolean(0.5),
				},
				{ kind: "taskComments", taskId, limit: rng.int(1, 3) },
				{
					kind: "projectTasks",
					projectId,
					limit: rng.int(1, 3),
					commentLimit: rng.int(1, 3),
				},
				{ kind: "inventory", collection: "memberships" },
			]
			if (this.options.initialSubscriptions === "scoped")
				choices.push({
					kind: "tasks",
					minPriority: 1,
					limit: 2,
					offset: 1,
					projection: true,
				})
			const spec = rng.pick(choices)
			return { type: "subscribe", client, id, spec }
		}
		if (
			writers.length > 0 &&
			world.mutationsInFlight < maxMutationsInFlight &&
			(pending.length === 0 || rng.boolean(mutateRate))
		) {
			if (rng.boolean(serverWriteRate)) {
				// Mostly its own records, as an agent streaming replies would, but
				// also ones clients share, so writers conflict.
				const id = rng.boolean(0.7)
					? rng.pick(serverOwnedIds)
					: `item-${rng.int(1, this.options.recordCount)}`
				if (rng.boolean(0.1)) return { type: "serverRemove", id }
				// Streams text into the record, as a reply would token by token.
				if (rng.boolean(0.6))
					return { type: "serverAppend", id, text: ` t${step}` }
				return {
					type: "serverSet",
					item: {
						id,
						projectId: `item-${rng.int(1, this.options.recordCount)}`,
						text: `Server ${id} (rev ${step})`,
						done: rng.boolean(0.3),
						priority: rng.int(1, 5),
					},
				}
			}
			const client = rng.pick(writers)
			if (client.startsWith("tab") && rng.boolean(toggleTabRate))
				return { type: "toggleTab", tab: client as DstTabName }
			if (
				this.options.namedAppendRate > 0 &&
				!client.startsWith("tab") &&
				rng.boolean(this.options.namedAppendRate)
			) {
				const tasks = world
					.inventoryOf(client as DstClientName)
					.tasks.filter(({ id }) => !serverOwnedIds.includes(id))
				if (tasks.length)
					return {
						type: "namedAppend",
						client: client as DstClientName,
						id: rng.pick(tasks).id,
						text: ` n${step}`,
						...(step % 4 === 0 && { field: "note" as const }),
					}
			}
			const ctx = { rng, world, step, client }
			const ops = this.chooseBatch(ctx)
			return { type: "mutate", client, ops }
		}
		if (pending.length > 0) {
			return { type: "advance", call: rng.pick(pending).name }
		}
		return undefined
	}

	private chooseBatch(ctx: {
		rng: SimPrng
		world: DstWorld
		step: number
		client: DstWriterName
	}): MutationOp<DstSchema>[] {
		const { rng, step, client } = ctx
		if (rng.boolean(0.5)) {
			return Array.from({ length: rng.int(1, 4) }, () =>
				this.chooseOperation(ctx),
			)
		}
		const prefix = rng.boolean(this.options.sharedWriteRate)
			? "item-"
			: `${client}-item-`
		const id = `${prefix}${rng.int(1, this.options.recordCount)}`
		const task: DstSchema["tasks"] = {
			id,
			projectId: id,
			text: `Task ${id} (rev ${step})`,
			done: false,
			priority: rng.int(1, 5),
		}
		if (rng.boolean(0.5))
			return [
				{
					type: "set",
					collection: "projects",
					value: { id, name: `Project ${id} (rev ${step})` },
				},
				{ type: "set", collection: "tasks", value: task },
				{
					type: "set",
					collection: "comments",
					value: { id, taskId: id, body: `Comment ${id} (rev ${step})` },
				},
			]
		const set: MutationOp<DstSchema> = {
			type: "set",
			collection: "tasks",
			value: task,
		}
		const remove: MutationOp<DstSchema> = {
			type: "remove",
			collection: "tasks",
			id,
		}
		return rng.pick([
			[
				set,
				{ type: "update", collection: "tasks", id, payload: { done: true } },
			],
			[set, remove],
			[remove, set],
		])
	}

	private chooseOperation({
		rng,
		world,
		step,
		client,
	}: {
		rng: SimPrng
		world: DstWorld
		step: number
		client: DstWriterName
	}): MutationOp<DstSchema> {
		const { recordCount, sharedWriteRate } = this.options
		const prefix = rng.boolean(sharedWriteRate) ? "item-" : `${client}-item-`
		const id = `${prefix}${rng.int(1, recordCount)}`
		const inventory = client.startsWith("tab")
			? world.tabs!.inventoryOf(client as DstTabName)
			: world.inventoryOf(client as DstClientName)
		// Most references resolve locally; missing ones exercise dangling relations.
		const reference = (collection: "projects" | "tasks", current?: string) => {
			const parents = inventory[collection].filter(({ id }) => id !== current)
			if (parents.length > 0 && rng.boolean(0.8)) return rng.pick(parents).id
			const missing = `missing-${prefix}${rng.int(1, recordCount)}`
			return missing === current ? `${missing}-other` : missing
		}
		const collection = rng.pick(dstCollections)
		const pickExisting = <Row extends DstSchema[keyof DstSchema]>(
			records: readonly Row[],
		) => {
			const candidates = records.filter(({ id }) =>
				(typeof id === "string" ? id : id[1]).startsWith(prefix),
			)
			return candidates.length > 0 ? rng.pick(candidates) : undefined
		}
		const remove = rng.boolean(0.2)
		const update = rng.boolean(0.5)
		switch (collection) {
			case "projects": {
				const project = pickExisting(inventory.projects)
				if (remove)
					return {
						type: "remove",
						collection,
						id: project && rng.boolean(0.8) ? project.id : id,
					}
				if (update && project)
					return {
						type: "update",
						collection,
						id: project.id,
						payload: { name: `Project ${project.id} (rev ${step})` },
					}
				return {
					type: "set",
					collection,
					value: { id, name: `Project ${id} (rev ${step})` },
				}
			}
			case "tasks": {
				const task = pickExisting(inventory.tasks)
				if (remove)
					return {
						type: "remove",
						collection,
						id: task && rng.boolean(0.8) ? task.id : id,
					}
				if (update && task) {
					const text = `Updated ${task.id} (rev ${step})`
					const done = !task.done
					const priority = rng.int(1, 5)
					return {
						type: "update",
						collection,
						id: task.id,
						payload: rng.pick([
							{ text },
							{ done },
							{ priority },
							{ text, done },
							{ text, priority },
							{ done, priority },
							{ projectId: reference("projects", task.projectId) },
						]),
					}
				}
				return {
					type: "set",
					collection,
					value: {
						id,
						projectId: reference("projects"),
						text: `Note ${id} (rev ${step})`,
						done: rng.boolean(0.3),
						priority: rng.int(1, 5),
					},
				}
			}
			case "comments": {
				const comment = pickExisting(inventory.comments)
				if (remove)
					return {
						type: "remove",
						collection,
						id: comment && rng.boolean(0.8) ? comment.id : id,
					}
				if (update && comment)
					return {
						type: "update",
						collection,
						id: comment.id,
						payload: rng.boolean(0.5)
							? { body: `Comment ${comment.id} (rev ${step})` }
							: { taskId: reference("tasks", comment.taskId) },
					}
				return {
					type: "set",
					collection,
					value: {
						id,
						taskId: reference("tasks"),
						body: `Comment ${id} (rev ${step})`,
					},
				}
			}
			case "memberships": {
				const membership = pickExisting(inventory.memberships)
				if (remove)
					return {
						type: "remove",
						collection,
						id:
							membership && rng.boolean(0.8)
								? membership.id
								: [reference("projects"), id],
					}
				const role = rng.pick(
					["reader", "editor", "owner"].filter(
						(role) => role !== membership?.role,
					),
				)
				if (update && membership)
					return {
						type: "update",
						collection,
						id: membership.id,
						payload: { role },
					}
				return {
					type: "set",
					collection,
					value: { id: [reference("projects"), id], role },
				}
			}
		}
	}
}
