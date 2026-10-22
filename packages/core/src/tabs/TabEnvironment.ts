export type TabMessageListener = (event: { data: unknown }) => void

/** The subset of BroadcastChannel that tab coordination uses. */
export type TabChannelApi = {
	postMessage(message: unknown): void
	addEventListener(type: "message", listener: TabMessageListener): void
	removeEventListener(type: "message", listener: TabMessageListener): void
	close(): void
}

/** The subset of the Web Locks API that tab coordination uses. */
export type TabLocksApi = {
	/**
	 * Holds an exclusive lock named `name` while `callback` runs. Requests for a
	 * held lock wait in order, and a closed tab's locks are released.
	 */
	request(name: string, callback: () => Promise<void>): Promise<void>
}

export type TabEnvironmentApi = {
	locks: TabLocksApi
	openChannel(name: string): TabChannelApi
}

type BrowserGlobals = {
	document?: unknown
	navigator?: { locks?: TabLocksApi }
	BroadcastChannel?: new (name: string) => TabChannelApi
}

/**
 * Web Locks and BroadcastChannel in a browser document, or undefined elsewhere.
 * Node also has both APIs, but its processes do not share IndexedDB.
 */
export function browserTabEnvironment(): TabEnvironmentApi | undefined {
	const globals = globalThis as BrowserGlobals
	const locks = globals.navigator?.locks
	const Channel = globals.BroadcastChannel
	if (!globals.document || !locks || !Channel) return undefined
	return {
		locks: { request: (name, callback) => locks.request(name, callback) },
		openChannel: (name) => new Channel(name),
	}
}

type InMemoryLock = {
	holder?: InMemoryTab
	waiting: { tab: InMemoryTab; grant: () => void }[]
}

/**
 * Browser tabs in one process, for tests. Tabs opened from the same instance
 * share locks and channels the way tabs of one origin do.
 */
export class InMemoryTabs {
	/** @internal */
	readonly locks = new Map<string, InMemoryLock>()
	/** @internal */
	readonly channels = new Set<InMemoryChannel>()

	openTab(): InMemoryTab {
		return new InMemoryTab(this)
	}
}

export class InMemoryTab implements TabEnvironmentApi {
	private closed = false

	constructor(private readonly tabs: InMemoryTabs) {}

	readonly locks: TabLocksApi = {
		request: async (name, callback) => {
			await this.acquire(name)
			if (this.closed) return
			try {
				await callback()
			} finally {
				this.release(name)
			}
		},
	}

	openChannel(name: string): TabChannelApi {
		const channel = new InMemoryChannel(this.tabs, name, this)
		if (this.closed) channel.close()
		else this.tabs.channels.add(channel)
		return channel
	}

	/**
	 * Ends the tab as a browser would: its locks and queued lock requests are
	 * dropped and its channels stop sending and receiving. Code already running
	 * in the tab is not stopped.
	 */
	close() {
		this.closed = true
		for (const [name, lock] of this.tabs.locks) {
			lock.waiting = lock.waiting.filter(({ tab }) => tab !== this)
			this.release(name)
		}
		for (const channel of this.tabs.channels) {
			if (channel.tab === this) channel.close()
		}
	}

	private acquire(name: string): Promise<void> {
		const lock = this.tabs.locks.get(name) ?? { waiting: [] }
		this.tabs.locks.set(name, lock)
		if (!lock.holder) {
			lock.holder = this
			return Promise.resolve()
		}
		return new Promise((grant) => lock.waiting.push({ tab: this, grant }))
	}

	private release(name: string) {
		const lock = this.tabs.locks.get(name)
		if (lock?.holder !== this) return
		const next = lock.waiting.shift()
		lock.holder = next?.tab
		next?.grant()
	}
}

class InMemoryChannel implements TabChannelApi {
	private readonly listeners = new Set<TabMessageListener>()
	private closed = false

	constructor(
		private readonly tabs: InMemoryTabs,
		readonly name: string,
		readonly tab: InMemoryTab,
	) {}

	postMessage(message: unknown) {
		if (this.closed) return
		for (const channel of this.tabs.channels) {
			if (channel === this || channel.name !== this.name) continue
			// Like BroadcastChannel: each receiver gets its own structured clone, later.
			const data = structuredClone(message)
			setTimeout(() => channel.deliver(data), 0)
		}
	}

	addEventListener(_type: "message", listener: TabMessageListener) {
		this.listeners.add(listener)
	}

	removeEventListener(_type: "message", listener: TabMessageListener) {
		this.listeners.delete(listener)
	}

	close() {
		this.closed = true
		this.tabs.channels.delete(this)
	}

	private deliver(data: unknown) {
		if (this.closed) return
		for (const listener of this.listeners) listener({ data })
	}
}
