import { expect, it } from "vitest"
import { ThrottleQueue } from "./ThrottleQueue.js"

it("flush runs each batch once and keeps newer work behind an active task", async () => {
	const ticks: (() => void)[] = []
	const started = Promise.withResolvers<void>()
	const release = Promise.withResolvers<void>()
	const saved: string[] = []
	let value = "first"
	const queue = new ThrottleQueue(
		async () => {
			const snapshot = value
			if (snapshot === "first") {
				started.resolve()
				await release.promise
			}
			saved.push(snapshot)
		},
		{
			waitForNextTick: () =>
				new Promise((resolve) => {
					ticks.push(resolve)
				}),
		},
	)

	// Flush starts the first batch without a tick, but does not report it durable early.
	const first = queue.enqueue()
	let flushed = false
	const flush = queue.flush().then(() => {
		flushed = true
	})
	await started.promise
	expect(flushed).toBe(false)

	// Firing the bypassed timer must neither duplicate that batch nor start newer work.
	value = "second"
	const second = queue.enqueue()
	ticks.shift()!()
	await new Promise<void>((resolve) => setImmediate(resolve))
	expect(saved).toEqual([])
	release.resolve()
	await Promise.all([first, flush])
	expect(saved).toEqual(["first"])

	// A second flush runs the new batch; its original timer cannot run it again.
	await queue.flush()
	await second
	for (const tick of ticks) tick()
	await new Promise<void>((resolve) => setImmediate(resolve))
	expect(saved).toEqual(["first", "second"])
})
