import { Gatekeeper } from "@tanishqkancharla/gatekeeper"
import {
	codec,
	collection,
	defineSchema,
	type LoggerApi,
	type RemoteApi,
	tag,
	type ClientId,
	type Cookie,
	type MutationId,
	TandemClient,
	type TandemClientStorageApi,
	type TandemClientTuple,
} from "@tanishqkancharla/tandem-core"
import * as errore from "errore"
import { assert, describe, expect } from "vitest"
import {
	buildGatekeeperHarness,
	expectQuery,
	test,
	todo,
	type DemoRng,
	type TestsSchema,
} from "./fixtures.js"

class EventStart {
	constructor(readonly iso: string) {}
}

type TestsEvent = {
	id: string
	title: string
	startAt: EventStart
}

type TestsEventStorageValue = {
	id: string
	title: string
	startAt: string
}

type TestsEventSchema = {
	events: TestsEvent
}

const eventCodec = codec<TestsEvent, TestsEventStorageValue>(
	"event",
	(input) => {
		const event = input as TestsEvent
		return {
			...event,
			startAt: event.startAt.iso,
		}
	},
	(input) => {
		const event = input as TestsEventStorageValue
		return {
			...event,
			startAt: new EventStart(event.startAt),
		}
	},
)

const testsEventRuntimeSchema = defineSchema({
	events: collection<TestsEvent, TestsEventStorageValue>({
		fields: ["id", "title", "startAt"],
		codec: eventCodec,
	}),
})

function buildClientGatekeeper<Client extends object>(client: Client) {
	return new Gatekeeper().add("client", () => client).build()
}

function createPersistentSyncGatekeeper({
	server,
	logger,
	rng,
	client1Storage,
}: {
	server: RemoteApi<TestsSchema>
	logger: LoggerApi
	rng: DemoRng
	client1Storage: TandemClientStorageApi<TestsSchema>
}) {
	const clients: TandemClient<TestsSchema>[] = []
	const gatekeeper = buildGatekeeperHarness({
		server,
		createClient: (remote, label) => {
			const client = new TandemClient<TestsSchema>({
				remote,
				clientStorage: label === "client1" ? client1Storage : undefined,
				logger,
				rng: rng.create(label),
				autoConnect: false,
				syncInterval: 0,
			})
			clients.push(client)
			return client
		},
	})
	return { clients, gatekeeper }
}

describe("TandemClient persistence", () => {
	test("round-trips replica metadata and ordered outbox operations alongside codec-backed records", async ({
		makeStorage,
		rng,
		logger,
	}) => {
		const dbName = rng.next("replica-tuples")
		const storage = makeStorage<TestsEventSchema>({
			dbName,
			schema: testsEventRuntimeSchema,
		})
		const event = {
			id: "event-1",
			title: "Confirmed",
			startAt: new EventStart("2026-05-04T12:00:00.000Z"),
		}
		const edited = {
			...event,
			title: "Pending",
			startAt: new EventStart("2026-05-05T16:30:00.000Z"),
		}
		const mutationId = tag<MutationId>(8)
		const tuples: TandemClientTuple<TestsEventSchema>[] = [
			{ key: ["record", "events", event.id], value: event },
			{
				key: ["replica"],
				value: {
					clientId: tag<ClientId>("client-a"),
					lastIssuedMutationId: mutationId,
					cookie: tag<Cookie>("view-b"),
				},
			},
			{
				key: ["outbox", mutationId],
				value: [
					{ type: "remove", collection: "events", id: "event-2" },
					{ type: "set", collection: "events", value: edited },
					{ type: "remove", collection: "events", id: event.id },
				],
			},
		]
		await storage.commit({ set: tuples })
		await storage.close()

		// A new adapter restores class instances inside records and outbox operations.
		const reopened = makeStorage<TestsEventSchema>({
			dbName,
			schema: testsEventRuntimeSchema,
		})
		const restored = await reopened.scan()
		expect(restored).toHaveLength(3)
		expect(restored).toEqual(expect.arrayContaining(tuples))
		const outbox = restored.find((tuple) => tuple.key[0] === "outbox")
		assert(outbox && Array.isArray(outbox.value))
		const set = outbox.value[1]
		assert(set?.type === "set")
		expect(set.value.startAt).toBeInstanceOf(EventStart)

		// Until sync hydration is implemented, only application records enter the visible database.
		await using harness = buildClientGatekeeper(
			new TandemClient<TestsEventSchema>({
				schema: testsEventRuntimeSchema,
				clientStorage: reopened,
				logger,
				autoConnect: false,
			}),
		)
		await harness.client.ready
		expect(harness.client.query({ collection: "events" })).toEqual([event])

		// Removing an outbox entry leaves records and metadata untouched.
		await reopened.commit({ remove: [["outbox", mutationId]] })
		expect(await reopened.scan()).toEqual(
			expect.arrayContaining(tuples.slice(0, 2)),
		)
		expect(await reopened.scan()).toHaveLength(2)
	})

	test("does not persist a batch prefix when an outbox codec fails", async ({
		makeStorage,
		rng,
	}) => {
		const storage = makeStorage<TestsEventSchema>({
			dbName: rng.next("failed-outbox-codec"),
			codecs: {
				events: codec(
					"reject",
					() => {
						throw new Error("Cannot encode event")
					},
					(value) => value as TestsEvent,
				),
			},
		})
		await expect(
			storage.commit({
				set: [
					{
						key: ["replica"],
						value: {
							clientId: tag<ClientId>("client-a"),
							lastIssuedMutationId: tag<MutationId>(1),
						},
					},
					{
						key: ["outbox", tag<MutationId>(1)],
						value: [
							{
								type: "set",
								collection: "events",
								value: {
									id: "event-1",
									title: "Rejected",
									startAt: new EventStart("2026-05-04"),
								},
							},
						],
					},
				],
			}),
		).rejects.toThrow("Cannot encode event")
		expect(await storage.scan()).toEqual([])
	})

	test("reloads persisted records after recreating the app", async ({
		logger,
		makeStorage,
		rng,
	}) => {
		const firstStorage = makeStorage<TestsSchema>({ dbName: "persisted-todos" })
		await using firstGatekeeper = buildClientGatekeeper(
			new TandemClient<TestsSchema>({
				clientStorage: firstStorage,
				logger,
				rng: rng.create("persistent-client-1"),
			}),
		)
		const firstClient = firstGatekeeper.client
		await firstClient.ready

		// Commit records with the first client
		const tx = firstClient.transact()
		tx.set(
			"todos",
			todo("todo-1", { text: "Write the sync spec", priority: 2 }),
		)
		tx.set("todos", todo("todo-3", { text: "Fix the sync bug", priority: 3 }))
		await (
			await firstClient.commit(tx)
		).result
		await (
			await firstClient.flushClientStorage()
		).result
		await firstStorage.close()

		// A new client backed by the same storage sees the persisted records
		await using secondGatekeeper = buildClientGatekeeper(
			new TandemClient<TestsSchema>({
				clientStorage: makeStorage<TestsSchema>({
					dbName: "persisted-todos",
				}),
				logger,
				rng: rng.create("persistent-client-2"),
			}),
		)
		const secondClient = secondGatekeeper.client
		await secondClient.ready

		const persistedTodosOnReload = secondClient.query({
			collection: "todos",
			orderBy: { priority: "asc" },
		})

		expect(persistedTodosOnReload).toEqual([
			todo("todo-1", { text: "Write the sync spec", priority: 2 }),
			todo("todo-3", { text: "Fix the sync bug", priority: 3 }),
		])
	})

	test("reloads persisted records through schema-owned codecs", async ({
		logger,
		makeStorage,
		rng,
	}) => {
		const dbName = rng.next("schema-codec-events")
		const firstStorage = makeStorage<TestsEventSchema>({
			dbName,
			schema: testsEventRuntimeSchema,
		})
		await using firstGatekeeper = buildClientGatekeeper(
			new TandemClient<TestsEventSchema>({
				schema: testsEventRuntimeSchema,
				relations: {},
				clientStorage: firstStorage,
				logger,
				rng: rng.create("schema-codec-client-1"),
			}),
		)
		const firstClient = firstGatekeeper.client
		await firstClient.ready

		// Commit an event whose runtime value relies on the schema-owned codec
		const event = {
			id: "event-1",
			title: "Planning",
			startAt: new EventStart("2026-05-04T12:00:00.000Z"),
		}
		const tx = firstClient.transact()
		tx.set("events", event)
		await (
			await firstClient.commit(tx)
		).result
		await (
			await firstClient.flushClientStorage()
		).result
		await firstStorage.close()

		await using secondGatekeeper = buildClientGatekeeper(
			new TandemClient<TestsEventSchema>({
				schema: testsEventRuntimeSchema,
				relations: {},
				clientStorage: makeStorage<TestsEventSchema>({
					dbName,
					schema: testsEventRuntimeSchema,
				}),
				logger,
				rng: rng.create("schema-codec-client-2"),
			}),
		)
		const secondClient = secondGatekeeper.client
		await secondClient.ready

		// Recreated storage decodes the persisted value back to the runtime shape
		const persistedEvents = secondClient.query({ collection: "events" })

		expect(persistedEvents).toEqual([event])
		expect(persistedEvents[0]?.startAt).toBeInstanceOf(EventStart)
	})

	test("keeps explicit IndexedDB codec support without a runtime schema", async ({
		logger,
		makeStorage,
		rng,
	}) => {
		const dbName = rng.next("explicit-codec-events")
		const firstStorage = makeStorage<TestsEventSchema>({
			dbName,
			codecs: { events: eventCodec },
		})
		await using firstGatekeeper = buildClientGatekeeper(
			new TandemClient<TestsEventSchema>({
				relations: {},
				clientStorage: firstStorage,
				logger,
				rng: rng.create("explicit-codec-client-1"),
			}),
		)
		const firstClient = firstGatekeeper.client
		await firstClient.ready

		// Existing explicit storage codecs still encode persisted writes
		const event = {
			id: "event-1",
			title: "Planning",
			startAt: new EventStart("2026-05-04T12:00:00.000Z"),
		}
		const tx = firstClient.transact()
		tx.set("events", event)
		await (
			await firstClient.commit(tx)
		).result
		await (
			await firstClient.flushClientStorage()
		).result
		await firstStorage.close()

		await using secondGatekeeper = buildClientGatekeeper(
			new TandemClient<TestsEventSchema>({
				relations: {},
				clientStorage: makeStorage<TestsEventSchema>({
					dbName,
					codecs: { events: eventCodec },
				}),
				logger,
				rng: rng.create("explicit-codec-client-2"),
			}),
		)
		const secondClient = secondGatekeeper.client
		await secondClient.ready

		// Recreated storage decodes through the explicit codec as before
		const persistedEvents = secondClient.query({ collection: "events" })

		expect(persistedEvents).toEqual([event])
		expect(persistedEvents[0]?.startAt).toBeInstanceOf(EventStart)
	})

	test("loads cached data before reconciling with a newer server", async ({
		logger,
		makeStorage,
		rng,
		server,
	}) => {
		const dbName = rng.next("reconciled-cache")
		const cachedTodo = todo("todo-1", { text: "Cached title" })
		const serverTodo = todo("todo-1", { text: "Newer server title" })

		// The first app instance syncs and persists the initial server value
		{
			const firstStorage = makeStorage<TestsSchema>({ dbName })
			const { clients, gatekeeper } = createPersistentSyncGatekeeper({
				server,
				logger,
				rng,
				client1Storage: firstStorage,
			})
			await using cleanup = new errore.AsyncDisposableStack()
			await using harness = gatekeeper
			for (const client of clients) {
				await client.ready
				await client.connect()
				cleanup.defer(() => client.disconnect())
			}
			const subscription = harness.client1.subscribe({ collection: "todos" })

			const seedTx = harness.client2.transact()
			seedTx.set("todos", cachedTodo)
			await (
				await harness.client2.commit(seedTx)
			).result
			await expectQuery(harness.client1, {
				collection: "todos",
			}).toResolveTo([cachedTodo])
			await (
				await harness.client1.flushClientStorage()
			).result
			await (
				await harness.client1.disconnect()
			).result

			// The server advances after the persisted client goes offline
			const updateTx = harness.client2.transact()
			updateTx.set("todos", serverTodo)
			await (
				await harness.client2.commit(updateTx)
			).result
			subscription.destroy()
			await firstStorage.close()
		}

		// A recreated app renders its cache, then reconciles after connecting
		const secondStorage = makeStorage<TestsSchema>({ dbName })
		const { clients, gatekeeper } = createPersistentSyncGatekeeper({
			server,
			logger,
			rng,
			client1Storage: secondStorage,
		})
		await using cleanup = new errore.AsyncDisposableStack()
		await using harness = gatekeeper
		await Promise.all(clients.map((client) => client.ready))

		expect(harness.client1.query({ collection: "todos" })).toEqual([cachedTodo])

		const subscription = harness.client1.subscribe({ collection: "todos" })
		await (
			await harness.client1.connect()
		).result
		cleanup.defer(() => clients[0]?.disconnect())

		expect(harness.client1.query({ collection: "todos" })).toEqual([serverTodo])
		subscription.destroy()
	})
})
