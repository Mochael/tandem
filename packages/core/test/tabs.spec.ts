import {
	codec,
	collection,
	defineSchema,
	InMemoryTabs,
	type AnySchema,
	type ClientApi,
	type InMemoryTab,
	type Mutation,
	type RemoteApi,
	type RuntimeSchemaDefinition,
	TandemClient,
	type TandemClientArgs,
	type TandemClientStorageApi,
	TandemClientIndexedDbStorage,
} from "@tanishqkancharla/tandem-core"
import { describe, expect } from "vitest"
import { test, todo, type TestsSchema } from "./fixtures.js"

type Tab<Schema extends AnySchema> = {
	client: TandemClient<Schema>
	/** Ends the tab mid-flight: no further storage, network, or channel I/O completes. */
	crash(): void
}

type OpenTabOptions<Schema extends AnySchema> = Pick<
	TandemClientArgs<Schema, {}>,
	"autoConnect"
> & {
	/** Runs after each storage commit completes, before the leader continues. */
	afterStorageCommit?: (
		writes: Parameters<TandemClientStorageApi<Schema>["commit"]>[0],
	) => void
}

const never = () => new Promise<never>(() => {})

function createTabs<Schema extends AnySchema>({
	server,
	schema,
	dbName,
	makeStorage,
	logger,
	rng,
}: {
	server: RemoteApi<Schema>
	schema?: RuntimeSchemaDefinition<Schema>
	dbName: string
	makeStorage: (options: {
		dbName: string
		schema?: RuntimeSchemaDefinition<Schema>
	}) => TandemClientIndexedDbStorage<Schema>
	logger: TandemClientArgs<Schema, {}>["logger"]
	rng: { create(prefix: string): { randomId(): string } }
}) {
	const browser = new InMemoryTabs()
	const opened: { client: TandemClient<Schema>; tab: InMemoryTab }[] = []
	/** Every mutation the server received, by sender. */
	const pushed: { clientId: string; mutation: Mutation<Schema> }[] = []
	/** The scan window of every pull the server received. */
	const pulls: { tab: string; scanWindow: unknown[] }[] = []
	/** Remote faults shared by every tab. */
	const remote = {
		down: false,
		pushHangs: false,
		connectDelayMs: 0,
		/** Pushes that reached the remote while it was stalled. */
		stalledPushes: 0,
	}
	const unavailable = {
		error: "unavailable",
		message: "Remote is down",
	} as const

	async function open(
		label: string,
		{ afterStorageCommit, ...options }: OpenTabOptions<Schema> = {},
	): Promise<Tab<Schema>> {
		const tab = browser.openTab()
		let alive = true
		const storage = makeStorage({ dbName, schema })
		const tabStorage: TandemClientStorageApi<Schema> = {
			name: storage.name,
			scan: (args) => (alive ? storage.scan(args) : never()),
			commit: async (writes) => {
				if (!alive) return never()
				await storage.commit(writes)
				afterStorageCommit?.(writes)
			},
			clear: () => (alive ? storage.clear() : never()),
			close: () => storage.close(),
		}
		const network: RemoteApi<Schema> = {
			connect: async (api: ClientApi) => {
				if (!alive) return never()
				if (remote.down) return unavailable
				if (remote.connectDelayMs)
					await new Promise((resolve) =>
						setTimeout(resolve, remote.connectDelayMs),
					)
				return server.connect({
					...api,
					poke: () => (alive ? api.poke() : Promise.resolve()),
				})
			},
			push: async (args) => {
				if (!alive) return never()
				if (remote.pushHangs) {
					remote.stalledPushes += 1
					return never()
				}
				if (remote.down) return unavailable
				for (const mutation of args.mutations)
					pushed.push({ clientId: args.clientId, mutation })
				return server.push(args)
			},
			pull: async (args) => {
				if (!alive) return never()
				if (remote.down) return unavailable
				pulls.push({ tab: label, scanWindow: args.scanWindow })
				return server.pull(args)
			},
		}
		const client = new TandemClient<Schema>({
			schema,
			remote: network,
			clientStorage: tabStorage,
			clientStorageWriteInterval: 0,
			syncInterval: 0,
			logger,
			rng: rng.create(label),
			tabs: { environment: tab },
			...options,
		})
		opened.push({ client, tab })
		await client.ready
		return {
			client,
			crash() {
				alive = false
				tab.close()
			},
		}
	}

	async function close() {
		for (const { client, tab } of opened.reverse()) {
			await Promise.race([
				client.close(),
				new Promise((resolve) => setTimeout(resolve, 100)),
			])
			tab.close()
		}
	}

	return { open, close, pushed, pulls, remote }
}

const tabsTest = test.extend<{
	tabs: ReturnType<typeof createTabs<TestsSchema>>
}>({
	tabs: async ({ server, makeStorage, logger, rng }, use) => {
		const tabs = createTabs<TestsSchema>({
			server,
			dbName: rng.next("shared-storage"),
			makeStorage,
			logger,
			rng,
		})
		await use(tabs)
		await tabs.close()
	},
})

describe("TandemClient tabs", () => {
	tabsTest(
		"tabs sharing storage deliver every write under their own client IDs",
		async ({ tabs, server }) => {
			const first = await tabs.open("tab1")
			first.client.subscribe({ collection: "todos" })
			const seedTx = first.client.transact()
			seedTx.set("todos", todo("a"))
			await first.client.commit(seedTx)

			// A tab opened after the first has written gets its own identity.
			const second = await tabs.open("tab2")
			second.client.subscribe({ collection: "todos" })
			expect(second.client.clientId).not.toBe(first.client.clientId)

			// Concurrent writes from both tabs all reach the server.
			const firstTx = first.client.transact()
			firstTx.set("todos", todo("b"))
			const secondTx = second.client.transact()
			secondTx.set("todos", todo("c"))
			await Promise.all([
				first.client.commit(firstTx),
				second.client.commit(secondTx),
			])
			const expected = [todo("a"), todo("b"), todo("c")]
			await expect
				.poll(() => server.query({ collection: "todos" }))
				.toEqual(expected)
			await expect
				.poll(() => first.client.query({ collection: "todos" }))
				.toEqual(expected)
			await expect
				.poll(() => second.client.query({ collection: "todos" }))
				.toEqual(expected)
		},
	)

	tabsTest(
		"a tab sees another tab's writes while the remote is unreachable",
		async ({ tabs, server }) => {
			const first = await tabs.open("tab1", { autoConnect: false })
			const second = await tabs.open("tab2", { autoConnect: false })
			first.client.subscribe({ collection: "todos" })
			second.client.subscribe({ collection: "todos" })

			const tx = second.client.transact()
			tx.set("todos", todo("offline", { text: "Written in tab 2" }))
			await second.client.commit(tx)

			await expect
				.poll(() => first.client.query({ collection: "todos" }))
				.toEqual([todo("offline", { text: "Written in tab 2" })])
			expect(await server.query({ collection: "todos" })).toEqual([])
		},
	)

	tabsTest(
		"offline writes from every tab survive closing all tabs and sync later",
		async ({ tabs, server }) => {
			const first = await tabs.open("tab1", { autoConnect: false })
			const second = await tabs.open("tab2", { autoConnect: false })

			// Each tab writes while the leader has never reached the remote.
			const firstTx = first.client.transact()
			firstTx.set("todos", todo("from-leader"))
			await first.client.commit(firstTx)
			const secondTx = second.client.transact()
			secondTx.set("todos", todo("from-follower"))
			await second.client.commit(secondTx)
			await second.client.close()
			await first.client.close()

			// A fresh tab restores both writes before it subscribes or connects.
			const reopened = await tabs.open("tab3", { autoConnect: false })
			const expected = [todo("from-follower"), todo("from-leader")]
			expect(reopened.client.query({ collection: "todos" })).toEqual(expected)
			expect(await server.query({ collection: "todos" })).toEqual([])

			// Connecting delivers both writes.
			await reopened.client.connect()
			await expect
				.poll(() => server.query({ collection: "todos" }))
				.toEqual(expected)
		},
	)

	tabsTest(
		"a follower takes over when the leader closes",
		async ({ tabs, server }) => {
			const first = await tabs.open("tab1")
			const second = await tabs.open("tab2")
			second.client.subscribe({ collection: "todos" })
			await first.client.close()

			const tx = second.client.transact()
			tx.set("todos", todo("after-handoff"))
			await second.client.commit(tx)
			await expect
				.poll(() => server.query({ collection: "todos" }))
				.toEqual([todo("after-handoff")])
		},
	)

	tabsTest(
		"a follower that resends after the leader crashes does not apply its write twice",
		async ({ tabs, server }) => {
			let crashLeader = () => {}
			const first = await tabs.open("tab1", {
				// Crash after the forwarded write and its session marker are saved,
				// before the follower hears that the leader has it.
				afterStorageCommit: (writes) => {
					if (writes.set?.some(({ key }) => key[0] === "session")) crashLeader()
				},
			})
			crashLeader = () => first.crash()
			const second = await tabs.open("tab2")
			second.client.subscribe({ collection: "todos" })

			const tx = second.client.transact()
			tx.set("todos", todo("once"))
			await second.client.commit(tx)

			await expect
				.poll(() => server.query({ collection: "todos" }))
				.toEqual([todo("once")])
			await expect
				.poll(() => second.client.query({ collection: "todos" }))
				.toEqual([todo("once")])
			// One leader mutation carries the write, however often it was sent.
			const carriers = new Set(
				tabs.pushed
					.filter(({ mutation }) =>
						mutation.ops.some(
							(op) => op.type === "set" && op.value.id === "once",
						),
					)
					.map(({ clientId, mutation }) => `${clientId}:${mutation.id}`),
			)
			expect(carriers.size).toBe(1)
		},
	)

	tabsTest(
		"a new leader keeps syncing what the tabs synced, without removing it first",
		async ({ tabs, server }) => {
			const first = await tabs.open("tab1")
			first.client.subscribe({ collection: "todos" })
			const tx = first.client.transact()
			tx.set("todos", todo("kept"))
			await first.client.commit(tx)
			await expect
				.poll(() => server.query({ collection: "todos" }))
				.toEqual([todo("kept")])

			const second = await tabs.open("tab2")
			second.client.subscribe({ collection: "todos" })
			await expect
				.poll(() => second.client.query({ collection: "todos" }))
				.toEqual([todo("kept")])

			// Every remote pull after the handoff carries tab 2's query, so the
			// server never answers with a removal for records tab 2 still syncs.
			await first.client.close()
			const pullsBeforeHandoff = tabs.pulls.length
			const tx2 = second.client.transact()
			tx2.set("todos", todo("after"))
			await second.client.commit(tx2)
			await expect
				.poll(() => server.query({ collection: "todos" }))
				.toEqual([todo("after"), todo("kept")])
			const handoffPulls = tabs.pulls.slice(pullsBeforeHandoff)
			expect(handoffPulls.length).toBeGreaterThan(0)
			expect(
				handoffPulls.every(({ scanWindow }) => scanWindow.length > 0),
			).toBe(true)
		},
	)

	tabsTest(
		"records a tab restored at startup are removed once deleted",
		async ({ tabs }) => {
			const first = await tabs.open("tab1", { autoConnect: false })
			const tx = first.client.transact()
			tx.set("todos", todo("deleted-later"))
			await first.client.commit(tx)

			// Tab 2 restores the record before subscribing to anything.
			const second = await tabs.open("tab2", { autoConnect: false })
			expect(second.client.query({ collection: "todos" })).toEqual([
				todo("deleted-later"),
			])

			const removeTx = first.client.transact()
			removeTx.remove("todos", "deleted-later")
			await first.client.commit(removeTx)
			second.client.subscribe({ collection: "todos" })
			await expect
				.poll(() => second.client.query({ collection: "todos" }))
				.toEqual([])
		},
	)

	tabsTest(
		"connect recovers after automatic connection failed",
		async ({ tabs, server }) => {
			tabs.remote.down = true
			const first = await tabs.open("tab1")
			const tx = first.client.transact()
			tx.set("todos", todo("queued"))
			await first.client.commit(tx)

			tabs.remote.down = false
			await first.client.connect()
			await expect
				.poll(() => server.query({ collection: "todos" }))
				.toEqual([todo("queued")])
		},
	)

	tabsTest(
		"commits, flushes, and handoff do not wait for a stalled remote",
		async ({ tabs }) => {
			const first = await tabs.open("tab1")
			const second = await tabs.open("tab2")
			tabs.remote.pushHangs = true

			const tx = second.client.transact()
			tx.set("todos", todo("saved"))
			await second.client.commit(tx)
			await second.client.flushClientStorage()

			// The leader saves and hands off; a reopened tab restores the write.
			await first.client.close()
			await second.client.close()
			const reopened = await tabs.open("tab3", { autoConnect: false })
			expect(reopened.client.query({ collection: "todos" })).toEqual([
				todo("saved"),
			])
		},
	)

	tabsTest(
		"one tab disconnecting leaves the leader connected for the others",
		async ({ tabs, server }) => {
			const first = await tabs.open("tab1")
			const second = await tabs.open("tab2")
			await second.client.disconnect()

			const tx = first.client.transact()
			tx.set("todos", todo("still-synced"))
			await first.client.commit(tx)
			await expect
				.poll(() => server.query({ collection: "todos" }))
				.toEqual([todo("still-synced")])
		},
	)

	tabsTest(
		"clear in one tab completes while other tabs hold the storage open",
		async ({ tabs, server }) => {
			const first = await tabs.open("tab1")
			const second = await tabs.open("tab2")
			const tx = first.client.transact()
			tx.set("todos", todo("before-clear"))
			await first.client.commit(tx)

			await first.client.clear()
			expect(first.client.query({ collection: "todos" })).toEqual([])

			// Sync still works for every tab afterwards.
			const afterTx = second.client.transact()
			afterTx.set("todos", todo("after-clear"))
			await second.client.commit(afterTx)
			await second.client.flushClientStorage()
			await expect
				.poll(() => server.query({ collection: "todos" }))
				.toEqual([todo("after-clear"), todo("before-clear")])
		},
	)

	tabsTest(
		"a closed leader stops syncing even if its connection finishes later",
		async ({ tabs }) => {
			tabs.remote.connectDelayMs = 50
			const first = await tabs.open("tab1")
			await tabs.open("tab2")
			await first.client.close()

			const pullsAfterClose = tabs.pulls.length
			await new Promise((resolve) => setTimeout(resolve, 200))
			expect(
				tabs.pulls.slice(pullsAfterClose).filter(({ tab }) => tab === "tab1"),
			).toEqual([])
		},
	)

	tabsTest(
		"clear does not wait for a stalled remote push",
		async ({ tabs }) => {
			const first = await tabs.open("tab1", { autoConnect: false })
			const tx = first.client.transact()
			tx.set("todos", todo("stalled"))
			await first.client.commit(tx)

			// Connecting pushes the queued write, which stalls at the remote.
			tabs.remote.pushHangs = true
			void first.client.connect()
			await expect.poll(() => tabs.remote.stalledPushes).toBeGreaterThan(0)
			await first.client.clear()
			expect(first.client.query({ collection: "todos" })).toEqual([])
		},
	)

	class EventStart {
		constructor(readonly iso: string) {}
	}

	type EventsSchema = {
		events: { id: string; startAt: EventStart }
	}

	const eventsSchema = defineSchema({
		events: collection<EventsSchema["events"], { id: string; startAt: string }>(
			{
				fields: ["id", "startAt"],
				codec: codec<EventsSchema["events"], { id: string; startAt: string }>(
					"event",
					(input) => {
						const event = input as EventsSchema["events"]
						return { id: event.id, startAt: event.startAt.iso }
					},
					(input) => {
						const event = input as { id: string; startAt: string }
						return { id: event.id, startAt: new EventStart(event.startAt) }
					},
				),
			},
		),
	}) as RuntimeSchemaDefinition<EventsSchema>

	test("collection codecs restore values that cross tabs", async ({
		makeRemote,
		makeStorage,
		logger,
		rng,
	}) => {
		const server = makeRemote({ schema: eventsSchema, relations: {} })
		const tabs = createTabs<EventsSchema>({
			server,
			schema: eventsSchema,
			dbName: rng.next("events-storage"),
			makeStorage: (options) => makeStorage<EventsSchema>(options),
			logger,
			rng,
		})
		try {
			const first = await tabs.open("tab1")
			const second = await tabs.open("tab2")
			first.client.subscribe({ collection: "events" })

			const tx = second.client.transact()
			tx.set("events", {
				id: "launch",
				startAt: new EventStart("2026-10-22T09:00:00.000Z"),
			})
			await second.client.commit(tx)
			await expect
				.poll(() => first.client.query({ collection: "events" }))
				.toHaveLength(1)
			const [event] = first.client.query({ collection: "events" })
			expect(event?.startAt).toBeInstanceOf(EventStart)
			expect(event?.startAt.iso).toBe("2026-10-22T09:00:00.000Z")
		} finally {
			await tabs.close()
		}
	})
})
