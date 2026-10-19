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

function createCheckpointGatekeeper({
	storage,
	server,
	logger,
	rng,
}: {
	storage: TandemClientStorageApi<TestsSchema>
	server: RemoteApi<TestsSchema>
	logger: LoggerApi
	rng: DemoRng
}) {
	return new Gatekeeper()
		.add("storage", () => storage)
		.add("server", () => ({
			connect: (client: Parameters<RemoteApi<TestsSchema>["connect"]>[0]) =>
				server.connect({ ...client, poke: async () => {} }),
			pull: (args: Parameters<RemoteApi<TestsSchema>["pull"]>[0]) =>
				server.pull(args),
			push: (args: Parameters<RemoteApi<TestsSchema>["push"]>[0]) =>
				server.push(args),
		}))
		.add("syncTimer", () => ({ waitForNextTick: () => Promise.resolve() }), {
			gates: { enter: false, exit: true },
		})
		.add(
			"client",
			({ storage, server, syncTimer }) =>
				new TandemClient<TestsSchema>({
					clientStorage: storage,
					clientStorageWriteInterval: 0,
					remote: server,
					syncInterval: syncTimer,
					autoConnect: false,
					logger,
					rng: rng.create("checkpoint-client"),
				}),
		)
		.build()
}

const checkpointTest = test.extend<{
	checkpoint: ReturnType<typeof createCheckpointGatekeeper>
	checkpointStorage: TandemClientStorageApi<TestsSchema>
}>({
	checkpointStorage: async ({ makeStorage }, use) => {
		await use(makeStorage<TestsSchema>())
	},
	checkpoint: async ({ checkpointStorage, server, logger, rng }, use) => {
		await using harness = createCheckpointGatekeeper({
			storage: checkpointStorage,
			server,
			logger,
			rng,
		})
		await harness.client.ready
		await (
			await harness.client.connect()
		).result
		harness.client.subscribe({ collection: "todos" })
		await (
			await harness.client.pullFromRemote()
		).result
		await use(harness)
		await harness.deactivateGatesAndSettle()
		await (
			await harness.client.disconnect()
		).result
		await (
			await harness.client.flushClientStorage()
		).result
	},
})

describe("TandemClient persistence", () => {
	checkpointTest(
		"persists a complete mutation before sending it and checkpoints pull acknowledgements atomically",
		async ({ checkpoint, checkpointStorage, server }) => {
			const { client } = checkpoint
			const record = todo("durable", { text: "Durable before delivery" })
			const tx = client.transact()
			tx.set("todos", record)
			await checkpoint.activateGates()
			const commit = await client.commit(tx)

			// The edit is optimistic, but the server cannot see it before the checkpoint completes.
			expect(client.query({ collection: "todos" })).toEqual([record])
			commit.assertWaitingFor("storage")
			expect(await server.query({ collection: "todos" })).toEqual([])
			await commit.continueTo("storage")
			expect(await checkpointStorage.scan()).toEqual(
				expect.arrayContaining([
					{ key: ["record", "todos", record.id], value: record },
					{
						key: ["base", "todos", record.id],
						value: { server: { type: "remove" }, lastWrittenBy: 1 },
					},
					{
						key: ["outbox", 1],
						value: [{ type: "set", collection: "todos", value: record }],
					},
					{
						key: ["replica"],
						value: {
							clientId: client.clientId,
							lastIssuedMutationId: 1,
							cookie: expect.anything(),
						},
					},
				]),
			)
			await commit.continueToCompletion()
			expect(await server.query({ collection: "todos" })).toEqual([record])

			// A failed pull checkpoint retains the previous cookie and pending mutation on disk.
			const authoritative = todo(record.id, {
				text: "Server corrected",
				priority: 4,
			})
			const serverTx = server.transact()
			serverTx.set("todos", authoritative)
			await server.commit(serverTx)
			const previous = await checkpointStorage.scan()
			const pull = await client.pullFromRemote()
			await pull.continueTo("client")
			await pull.continueTo("server")
			await pull.continueTo("client")
			pull.assertWaitingFor("storage")
			await pull.fail(new Error("Checkpoint unavailable"))
			await expect(pull.result).rejects.toThrow("Client storage commit failed")
			expect(await checkpointStorage.scan()).toEqual(previous)

			// An explicit flush retries the entire checkpoint, not acknowledgement pruning alone.
			const flush = await client.flushClientStorage()
			await flush.continueToCompletion()
			const saved = await checkpointStorage.scan()
			expect(
				saved.filter(({ key }) => key[0] === "outbox" || key[0] === "base"),
			).toEqual([])
			expect(saved).toEqual(
				expect.arrayContaining([
					{ key: ["record", "todos", record.id], value: authoritative },
					{
						key: ["replica"],
						value: {
							clientId: client.clientId,
							lastIssuedMutationId: 1,
							cookie: expect.anything(),
						},
					},
				]),
			)
			expect(saved.find(({ key }) => key[0] === "replica")).not.toEqual(
				previous.find(({ key }) => key[0] === "replica"),
			)
		},
	)

	checkpointTest(
		"retains failed and newer storage batches in order before delivering either mutation",
		async ({ checkpoint, checkpointStorage, server }) => {
			const { client } = checkpoint
			const first = todo("first", { text: "Must not disappear" })
			const final = todo("first", { text: "Newest version", priority: 7 })
			const second = todo("second", { text: "Arrived while saving" })
			const firstTx = client.transact()
			firstTx.set("todos", first)
			await checkpoint.activateGates()
			const failed = await client.commit(firstTx)
			failed.assertWaitingFor("storage")
			const nextTx = client.transact()
			nextTx.remove("todos", first.id)
			nextTx.set("todos", final)
			nextTx.set("todos", second)
			const nextReady = client.commit(nextTx)

			// The second batch must wait; the failed first write must not reach the server.
			expect(await server.query({ collection: "todos" })).toEqual([])
			await failed.fail(new Error("Disk temporarily unavailable"))
			await expect(failed.result).rejects.toThrow(
				"Client storage commit failed",
			)
			const next = await nextReady
			next.assertWaitingFor("storage")
			await next.continueTo("storage")
			const saved = await checkpointStorage.scan()
			expect(saved).toEqual(
				expect.arrayContaining([
					{ key: ["record", "todos", final.id], value: final },
					{ key: ["record", "todos", second.id], value: second },
					{
						key: ["outbox", 1],
						value: [{ type: "set", collection: "todos", value: first }],
					},
					{
						key: ["outbox", 2],
						value: [
							{ type: "remove", collection: "todos", id: first.id },
							{ type: "set", collection: "todos", value: final },
							{ type: "set", collection: "todos", value: second },
						],
					},
				]),
			)
			await next.continueToCompletion()
			expect(await server.query({ collection: "todos" })).toEqual([
				final,
				second,
			])
		},
	)

	checkpointTest(
		"waits for newer mutations included in an older queued push",
		async ({ checkpoint, checkpointStorage, server }) => {
			const { client } = checkpoint
			const first = todo("first")
			const second = todo("second")
			const firstTx = client.transact()
			firstTx.set("todos", first)
			await checkpoint.activateGates()
			const commit = await client.commit(firstTx)
			await commit.continueTo("storage")
			await commit.continueTo("client")
			commit.assertSentBy("syncTimer")
			const secondTx = client.transact()
			secondTx.set("todos", second)
			const newer = await client.commit(secondTx)
			newer.assertWaitingFor("storage")

			// Release the older push's timer while the newer mutation is still undurable.
			const pushing = commit.continueTo("client")
			expect(await server.query({ collection: "todos" })).toEqual([])
			expect(
				(await checkpointStorage.scan()).filter(
					({ key }) => key[0] === "outbox",
				),
			).toHaveLength(1)
			expect(
				checkpoint
					.pendingCalls()
					.filter(({ waitingFor }) => waitingFor === "server"),
			).toEqual([])
			await newer.continueTo("storage")
			const finishingNewer = newer.continueToCompletion()
			await pushing
			await commit.continueToCompletion()
			await finishingNewer
			expect(await server.query({ collection: "todos" })).toEqual([
				first,
				second,
			])
		},
	)

	checkpointTest(
		"flush waits for an active checkpoint and reports its failure",
		async ({ checkpoint, checkpointStorage, server }) => {
			const { client } = checkpoint
			const record = todo("flushed")
			const tx = client.transact()
			tx.set("todos", record)
			await checkpoint.activateGates()
			const commit = await client.commit(tx)
			commit.assertWaitingFor("storage")
			let flushed = false
			const flushReady = client.flushClientStorage().then((call) => {
				flushed = true
				return call
			})
			await checkpointStorage.scan()
			expect(flushed).toBe(false)

			// Both public callers see the storage error, and no network mutation escaped.
			await commit.fail(new Error("Storage failed during flush"))
			await expect(commit.result).rejects.toThrow(
				"Client storage commit failed",
			)
			const flush = await flushReady
			await expect(flush.result).rejects.toThrow("Client storage commit failed")
			expect(await server.query({ collection: "todos" })).toEqual([])

			// Retrying the flush saves the pending write without silently delivering it.
			const retry = await client.flushClientStorage()
			await retry.continueToCompletion()
			expect(await checkpointStorage.scan()).toContainEqual({
				key: ["record", "todos", record.id],
				value: record,
			})
			expect(await server.query({ collection: "todos" })).toEqual([])
		},
	)

	checkpointTest(
		"orders clear after an active save and never sends its old-session mutation",
		async ({ checkpoint, checkpointStorage, server }) => {
			const { client } = checkpoint
			const oldClientId = client.clientId
			const oldTx = client.transact()
			oldTx.set("todos", todo("discarded"))
			await checkpoint.activateGates()
			const old = await client.commit(oldTx)
			await old.continueTo("storage")
			await old.continueTo("client")
			const newerTx = client.transact()
			newerTx.set("todos", todo("also-discarded"))
			const newer = await client.commit(newerTx)
			newer.assertWaitingFor("storage")
			const pushing = old.continueTo("client")
			const clearing = client.clear()
			await newer.continueToCompletion()
			await pushing
			await old.continueToCompletion()
			const clear = await clearing
			await clear.continueToCompletion()

			// The completed old save cannot resurrect records after clear or use the new identity.
			expect(client.clientId).not.toBe(oldClientId)
			expect(client.query({ collection: "todos" })).toEqual([])
			expect(await server.query({ collection: "todos" })).toEqual([])
			expect(await checkpointStorage.scan()).toEqual([
				{
					key: ["replica"],
					value: { clientId: client.clientId, lastIssuedMutationId: 0 },
				},
			])

			// The new session starts at mutation 1 and can deliver normally.
			const record = todo("kept")
			const nextTx = client.transact()
			nextTx.set("todos", record)
			const next = await client.commit(nextTx)
			await next.continueToCompletion()
			expect(await server.query({ collection: "todos" })).toEqual([record])
		},
	)

	test("round-trips replica metadata, sparse bases, and ordered outbox operations alongside codec-backed records", async ({
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
				key: ["base", "events", event.id],
				value: {
					server: { type: "set", value: event },
					lastWrittenBy: mutationId,
				},
			},
			{
				key: ["base", "events", "event-2"],
				value: { server: { type: "remove" }, lastWrittenBy: mutationId },
			},
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

		// A new adapter restores class instances in bases and outbox operations, and explicit absence.
		const reopened = makeStorage<TestsEventSchema>({
			dbName,
			schema: testsEventRuntimeSchema,
		})
		const restored = await reopened.scan()
		expect(restored).toHaveLength(5)
		expect(restored).toEqual(expect.arrayContaining(tuples))
		const outbox = restored.find((tuple) => tuple.key[0] === "outbox")
		assert(outbox && Array.isArray(outbox.value))
		const set = outbox.value[1]
		assert(set?.type === "set")
		expect(set.value.startAt).toBeInstanceOf(EventStart)
		const base = restored.find(
			(tuple) => tuple.key[0] === "base" && tuple.key[2] === event.id,
		)
		assert(base && "server" in base.value && base.value.server.type === "set")
		expect(base.value.server.value.startAt).toBeInstanceOf(EventStart)

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

		// Removing an outbox entry leaves records, bases, and metadata untouched.
		await reopened.commit({ remove: [["outbox", mutationId]] })
		expect(await reopened.scan()).toEqual(
			expect.arrayContaining(
				tuples.filter((tuple) => tuple.key[0] !== "outbox"),
			),
		)
		expect(await reopened.scan()).toHaveLength(4)
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
