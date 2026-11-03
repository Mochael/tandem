import { appendFileSync, writeFileSync } from "node:fs"
import type { DstRunOptions } from "./DstSimulation.js"
import type { TandemServerStorageApi } from "@tanishqkancharla/tandem-server"
import type { DstSchema } from "./DstWorld.js"
import { recoveryOutcome, type DstRecoveryResult } from "./DstRecovery.js"
import { isDeepStrictEqual } from "node:util"
import {
	type DstIntent,
	type DstOutcome,
	type DstTraceRecord,
	type DstViolation,
	DstWorld,
} from "./DstWorld.js"

/**
 * One line of a run's JSONL artifact: a header, then one event per applied
 * step, then the outcome. A run that hangs or throws leaves the lines it wrote.
 */
export type DstArtifactLine =
	| { kind: "header"; format: 1; options: Required<DstRunOptions> }
	| ({ kind: "event" } & DstTraceRecord)
	| ({
			kind: "outcome"
			stepsCompleted: number
			recovery?: DstRecoveryResult
	  } & DstOutcome)

export type DstArtifactSink = { write(line: DstArtifactLine): void }

export type DstArtifact = {
	options: Required<DstRunOptions>
	trace: DstTraceRecord[]
	/** Missing when the run stopped before writing it. */
	outcome?: {
		stepsCompleted: number
		recovery?: DstRecoveryResult
	} & DstOutcome
}

/** Keeps lines in memory, for tests. */
export function memorySink(): DstArtifactSink & { lines: DstArtifactLine[] } {
	const lines: DstArtifactLine[] = []
	return { lines, write: (line) => lines.push(line) }
}

/** Appends each line to `path` as it is written, so a failed run leaves its trace. */
export function jsonlFileSink(path: string): DstArtifactSink {
	writeFileSync(path, "")
	return {
		write: (line) => appendFileSync(path, `${JSON.stringify(line)}\n`),
	}
}

export function formatArtifact(lines: readonly DstArtifactLine[]): string {
	return lines.map((line) => `${JSON.stringify(line)}\n`).join("")
}

export function parseArtifact(jsonl: string): DstArtifact {
	const lines = jsonl
		.split("\n")
		.filter((line) => line.trim() !== "")
		.map((line) => JSON.parse(line) as DstArtifactLine)
	const header = lines[0]
	if (header?.kind !== "header") {
		throw new Error("A DST artifact must start with its header")
	}
	const trace: DstTraceRecord[] = []
	let outcome: DstArtifact["outcome"]
	for (const line of lines.slice(1)) {
		if (line.kind === "event") {
			const { kind: _kind, ...record } = line
			trace.push(record as DstTraceRecord)
		} else if (line.kind === "outcome") {
			const { kind: _kind, ...rest } = line
			outcome = rest
		}
	}
	return { options: header.options, trace, outcome }
}

/** Where a replay left the recorded path: the recorded event could not apply as recorded. */
export type DstDivergence = {
	step: number
	expected: DstTraceRecord
	actual?: DstTraceRecord
	reason: string
}

export type DstReplayResult = {
	stepsCompleted: number
	violation: DstViolation | undefined
	divergence: DstDivergence | undefined
	recovery?: DstRecoveryResult
}

/**
 * Re-applies a recorded run's events, step by step, with no random choices.
 * Each event must apply exactly as recorded, including the boundary it acted
 * on and the ids it generated; otherwise the replay stops with a divergence.
 */
export async function replay(
	artifact: DstArtifact,
	serverStorage?: TandemServerStorageApi<DstSchema>,
): Promise<DstReplayResult> {
	await using world = await DstWorld.start({
		...artifact.options,
		serverStorage,
	})
	const events = new Map(artifact.trace.map((record) => [record.step, record]))
	const steps = Math.max(
		artifact.outcome?.stepsCompleted ?? 0,
		(artifact.trace.at(-1)?.step ?? -1) + 1,
	)
	let ordinary: DstOutcome | undefined
	let repaired: DstOutcome | undefined
	let eventsDelivered = 0

	const stepsCompleted =
		artifact.outcome?.stepsCompleted ??
		Math.min(artifact.options.steps, (artifact.trace.at(-1)?.step ?? -1) + 1)
	for (let step = 0; step < steps; step++) {
		world.pending()
		const expected = events.get(step)
		if (expected) {
			const reason = cannotApply(world, expected)
			if (reason) {
				await world.drain()
				return {
					stepsCompleted: step,
					violation: undefined,
					divergence: { step, expected, reason },
				}
			}
			const actual = await world.apply(step, intentOf(expected))
			// JSONL omits undefined object fields; key insertion order is not semantic.
			if (!isDeepStrictEqual(JSON.parse(JSON.stringify(actual)), expected)) {
				await world.drain()
				return {
					stepsCompleted: step,
					violation: undefined,
					divergence: {
						step,
						expected,
						actual,
						reason: "the event applied differently",
					},
				}
			}
			if (actual.type === "checkpoint") ordinary = actual.outcome
			if (actual.type === "repair") repaired = actual.outcome
			if (
				step >= artifact.options.steps &&
				(actual.type === "advance" ||
					actual.type === "drop" ||
					actual.type === "serverTick")
			)
				eventsDelivered++
		}
		await world.settle()
	}

	if (!repaired) await world.drain()
	if (artifact.outcome && !repaired)
		throw new Error("Completed DST artifact has no repair checkpoint")
	const outcome = repaired
		? recoveryOutcome(ordinary, repaired, eventsDelivered)
		: undefined
	if (
		artifact.outcome &&
		outcome &&
		!isDeepStrictEqual(
			JSON.parse(JSON.stringify({ stepsCompleted, ...outcome })),
			artifact.outcome,
		)
	) {
		const expected = artifact.trace.at(-1)!
		return {
			stepsCompleted,
			violation: outcome.violation,
			divergence: {
				step: expected.step,
				expected,
				reason: "the final outcome differs from the recording",
			},
		}
	}
	return {
		stepsCompleted,
		violation: outcome?.violation,
		divergence: undefined,
		recovery: outcome?.recovery,
	}
}

function cannotApply(
	world: DstWorld,
	record: DstTraceRecord,
): string | undefined {
	switch (record.type) {
		case "advance":
		case "drop":
			return world.hasPending(record.call)
				? undefined
				: `${record.call} is not held at a boundary`
		case "mutate":
		case "namedAppend":
		case "subscribe":
		case "unsubscribe":
		case "disconnect":
		case "reconnect":
		case "partition":
			return world.writers.includes(record.client)
				? undefined
				: `${record.client} cannot take writes`
		case "serverSet":
		case "serverAppend":
		case "serverRemove":
			return undefined
		case "serverTick":
			return world.serverTicksPending > 0
				? undefined
				: "no server interval is waiting"
		case "crash":
			return world.crashable.includes(record.client)
				? undefined
				: `${record.client} is not running`
		case "restart":
			return world.down.includes(record.client)
				? undefined
				: `${record.client} is not down`
		case "checkpoint":
		case "repair":
		case "heal":
		case "openTab":
			return undefined
		case "toggleTab":
		case "flushTab":
		case "connectTab":
			return world.tabs?.alive.includes(record.tab)
				? undefined
				: `${record.tab} is not open`
	}
}

function intentOf(record: DstTraceRecord): DstIntent {
	switch (record.type) {
		case "serverSet":
			return { type: "serverSet", item: record.item }
		case "serverAppend":
			return { type: "serverAppend", id: record.id, text: record.text }
		case "serverTick":
			return { type: "serverTick" }
		case "serverRemove":
			return { type: "serverRemove", id: record.id }
		case "mutate": {
			const { step: _step, mutationId: _mutationId, ...intent } = record
			return intent
		}
		case "namedAppend": {
			const { step: _step, mutationId: _mutationId, ...intent } = record
			return intent
		}
		case "subscribe":
		case "unsubscribe": {
			const { step: _step, ...intent } = record
			return intent
		}
		case "advance":
		case "drop":
			return { type: record.type, call: record.call }
		case "crash":
			return { type: "crash", client: record.client }
		case "restart":
		case "disconnect":
		case "reconnect":
		case "heal":
			return { type: record.type, client: record.client }
		case "partition":
			return {
				type: record.type,
				client: record.client,
				direction: record.direction,
			}
		case "checkpoint":
		case "repair":
		case "openTab":
			return { type: record.type }
		case "toggleTab":
			return { type: "toggleTab", tab: record.tab }
		case "flushTab":
			return { type: "flushTab", tab: record.tab }
		case "connectTab":
			return { type: "connectTab", tab: record.tab }
	}
}
