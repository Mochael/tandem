import type { TimerApi } from "./Timer.js"

export class ThrottleQueue<Result = void> {
	private pending?: { promise: Promise<Result>; start: () => void }
	private tail: Promise<Result | undefined> = Promise.resolve(undefined)

	constructor(
		private readonly task: () => Promise<Result>,
		private readonly timer: TimerApi,
	) {}

	/**
	 * Coalesce queued work, but serialize batches requested during an active task.
	 */
	enqueue(): Promise<Result> {
		if (this.pending) return this.pending.promise

		const immediate = Promise.withResolvers<void>()
		const delay = Promise.race([
			this.timer.waitForNextTick(),
			immediate.promise,
		])
		// An unexpected rejection still reaches its callers without poisoning later work.
		const previous = this.tail.then(
			() => undefined,
			() => undefined,
		)
		const promise = Promise.all([delay, previous]).then(() => {
			this.pending = undefined
			return this.task()
		})
		this.pending = { promise, start: () => immediate.resolve() }
		this.tail = promise
		return promise
	}

	/**
	 * Release the pending delay and await all work queued so far, including an active task.
	 * The original timer only resolves the same delay; it cannot execute the task twice.
	 */
	flush(): Promise<Result | undefined> {
		this.pending?.start()
		return this.tail
	}
}
