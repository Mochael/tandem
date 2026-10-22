import {
	collection,
	defineRelations,
	defineSchema,
	tag,
} from "@tanishqkancharla/tandem-core"
import type {
	ClientId,
	Mutation,
	MutationId,
	Patch,
	RemoteApi,
	ScanWindow,
} from "@tanishqkancharla/tandem-core"
import { assert, expect, expectTypeOf, test, vi } from "vitest"
import { TandemServer } from "../src/index.js"
import { TestTandemServerStorage } from "./TandemServerStorage.fixture.js"

type User = {
	id: string
	name: string
}

type Thread = {
	id: string
	ownerId: string
	title: string
	status: "open" | "closed"
	createdAt: number
}

type Message = {
	id: string
	threadId: string
	body: string
	createdAt: number
}

type Entry = {
	id: readonly [sessionId: string, entryId: number]
	body: string
}

type TestSchema = {
	users: User
	threads: Thread
	messages: Message
	entries: Entry
}

const schema = defineSchema({
	users: collection<User>({ fields: ["id", "name"] }),
	threads: collection<Thread>({
		fields: ["id", "ownerId", "title", "status", "createdAt"],
	}),
	messages: collection<Message>({
		fields: ["id", "threadId", "body", "createdAt"],
	}),
	entries: collection<Entry>({ fields: ["id", "body"] }),
})

const relations = defineRelations(schema, ({ one, many }) => ({
	users: {
		threads: many("threads", { from: "id", to: "ownerId" }),
	},
	threads: {
		owner: one("users", { from: "ownerId", to: "id" }),
		messages: many("messages", { from: "id", to: "threadId" }),
	},
	messages: {
		thread: one("threads", { from: "threadId", to: "id" }),
	},
}))

type TestRelations = typeof relations

function createServer() {
	const storage = new TestTandemServerStorage<TestSchema>()
	const server = new TandemServer({ schema, relations, storage })
	return { server, storage }
}

async function seed(server: TandemServer<TestSchema, TestRelations>) {
	const tx = server.transact()
	tx.set("users", { id: "user-1", name: "Ada" })
	tx.set("threads", {
		id: "thread-1",
		ownerId: "user-1",
		title: "Alpha",
		status: "open",
		createdAt: 1,
	})
	tx.set("threads", {
		id: "thread-2",
		ownerId: "user-1",
		title: "Zulu",
		status: "closed",
		createdAt: 2,
	})
	tx.set("threads", {
		id: "thread-3",
		ownerId: "user-1",
		title: "Bravo",
		status: "open",
		createdAt: 3,
	})
	tx.set("messages", {
		id: "message-1",
		threadId: "thread-1",
		body: "First",
		createdAt: 1,
	})
	tx.set("messages", {
		id: "message-2",
		threadId: "thread-1",
		body: "Latest",
		createdAt: 2,
	})
	await server.commit(tx)
}

function patchSetKeys(patch: Patch<TestSchema>) {
	return (patch.set ?? [])
		.map((operation) => `${operation.collection}.${operation.value.id}`)
		.toSorted()
}

function patchRemoveKeys(patch: Patch<TestSchema>) {
	return (patch.remove ?? [])
		.map((operation) => `${operation.collection}.${operation.id}`)
		.toSorted()
}

test("lost subscription responses and server restarts reset an unknown view", async () => {
	const { server, storage } = createServer()
	await seed(server)
	const clientId = tag<ClientId>("reset-client")
	const initial = await server.pull({
		clientId,
		scanWindow: [{ collection: "users" }],
	})
	assert(!("error" in initial))
	expect(initial.patch.reset).toBe(true)

	// The subscription changes without a database revision change, and its response is lost.
	const lost = await server.pull({
		clientId,
		cookie: initial.cookie,
		scanWindow: [],
	})
	assert(!("error" in lost))
	expect(lost.cookie).not.toBe(initial.cookie)
	expect(lost.patch.remove).toEqual([{ collection: "users", id: "user-1" }])
	const retry = await server.pull({
		clientId,
		cookie: initial.cookie,
		scanWindow: [],
	})
	assert(!("error" in retry))
	expect(retry.patch).toEqual({ reset: true, set: [], remove: [] })

	// Losing a reset is recoverable too, with a nonempty authoritative snapshot.
	const reset = await server.pull({
		clientId,
		cookie: initial.cookie,
		scanWindow: [{ collection: "users" }],
	})
	assert(!("error" in reset))
	expect(reset.patch).toEqual({
		reset: true,
		set: [{ collection: "users", value: { id: "user-1", name: "Ada" } }],
		remove: [],
	})

	// A new server instance must not accept the old instance's cookie.
	const restarted = new TandemServer({ schema, relations, storage })
	const recovered = await restarted.pull({
		clientId,
		cookie: reset.cookie,
		scanWindow: [{ collection: "users" }],
	})
	assert(!("error" in recovered))
	expect(recovered.patch).toEqual(reset.patch)
	expect(recovered.cookie).not.toBe(reset.cookie)
	await restarted.close()
	await server.close()
})

test("pull reads external writes with the same cookie and recovers a lost removal response", async () => {
	const { server, storage } = createServer()
	const seedTx = server.transact()
	seedTx.set("users", { id: "user-1", name: "Ada" })
	seedTx.set("users", { id: "user-2", name: "Grace" })
	await server.commit(seedTx)
	const clientId = tag<ClientId>("external-writes-client")
	const scanWindow: ScanWindow<TestSchema> = [{ collection: "users" }]
	const initial = await server.pull({ clientId, scanWindow })
	assert(!("error" in initial))

	// External writes do not pass through the server or change the subscription window.
	await storage.commit({
		set: [
			{
				key: ["record", "users", "user-1"],
				value: { id: "user-1", name: "Updated Ada" },
			},
			{
				key: ["record", "users", "user-3"],
				value: { id: "user-3", name: "Lin" },
			},
		],
		remove: [["record", "users", "user-2"]],
	})
	const changed = await server.pull({
		clientId,
		scanWindow,
		cookie: initial.cookie,
	})
	assert(!("error" in changed))
	const expectedSet = [
		{ collection: "users", value: { id: "user-1", name: "Updated Ada" } },
		{ collection: "users", value: { id: "user-3", name: "Lin" } },
	]
	expect(changed.patch).toEqual({
		set: expectedSet,
		remove: [{ collection: "users", id: "user-2" }],
	})
	expect(changed.cookie).not.toBe(initial.cookie)
	expect(changed.lastMutationId).toBe(0)

	// If the response was lost, retrying its old cookie still delivers the deletion by reset.
	const retry = await server.pull({
		clientId,
		scanWindow,
		cookie: initial.cookie,
	})
	assert(!("error" in retry))
	expect(retry.patch).toEqual({ reset: true, set: expectedSet, remove: [] })
	await server.close()
})

test("transactions provide typed CRUD and read their staged writes", async () => {
	const { server, storage } = createServer()
	const tx = server.transact()

	tx.set("threads", {
		id: "thread-1",
		ownerId: "user-1",
		title: "Draft",
		status: "open",
		createdAt: 1,
	})
	expect(await tx.get("threads", "thread-1")).toEqual({
		id: "thread-1",
		ownerId: "user-1",
		title: "Draft",
		status: "open",
		createdAt: 1,
	})
	expect(await tx.list("threads")).toHaveLength(1)
	await tx.update("threads", "thread-1", (thread) => ({
		...thread,
		title: "Published",
	}))
	await server.commit(tx)

	const verify = server.transact()
	expect(await verify.get("threads", "thread-1")).toMatchObject({
		title: "Published",
	})
	verify.remove("threads", "thread-1")
	expect(await verify.get("threads", "thread-1")).toBeUndefined()
	await verify.cancel()

	await server.close()
	expect(storage.closed).toBe(true)
})

test("compound IDs support exact reads and ordered prefix scans", async () => {
	const { server } = createServer()
	const tx = server.transact()

	tx.set("entries", { id: ["session-2", 1], body: "Other session" })
	tx.set("entries", { id: ["session-1", 2], body: "Second" })
	tx.set("entries", { id: ["session-1", 1], body: "First" })

	expect(await tx.get("entries", ["session-1", 2])).toEqual({
		id: ["session-1", 2],
		body: "Second",
	})
	expect(await tx.scan("entries", { prefix: ["session-1"] })).toEqual([
		{ id: ["session-1", 1], body: "First" },
		{ id: ["session-1", 2], body: "Second" },
	])
	expect(
		await tx.scan("entries", {
			prefix: ["session-1"],
			reverse: true,
			limit: 1,
		}),
	).toEqual([{ id: ["session-1", 2], body: "Second" }])

	await tx.update("entries", ["session-1", 1], (entry) => ({
		...entry,
		body: "Updated",
	}))
	tx.remove("entries", ["session-1", 2])
	await server.commit(tx)

	const verifyTx = server.transact()
	expect(await verifyTx.scan("entries", { prefix: ["session-1"] })).toEqual([
		{ id: ["session-1", 1], body: "Updated" },
	])
	await verifyTx.cancel()
})

test("compound prefix scans participate in read/write conflicts", async () => {
	const { server } = createServer()
	const readerTx = server.transact()
	expect(await readerTx.scan("entries", { prefix: ["session-1"] })).toEqual([])

	const writerTx = server.transact()
	writerTx.set("entries", { id: ["session-1", 1], body: "Concurrent" })
	await server.commit(writerTx)

	readerTx.set("users", { id: "user-1", name: "Ada" })
	await expect(server.commit(readerTx)).rejects.toThrow(
		"Tandem server commit failed",
	)
})

test("compound record writes invalidate query subscriptions", async () => {
	const { server } = createServer()
	const results: Entry[][] = []
	const subscription = await server.subscribe(
		{ collection: "entries" },
		(result) => results.push(result),
	)

	const tx = server.transact()
	tx.set("entries", { id: ["session-1", 1], body: "First" })
	await server.commit(tx)

	expect(results.at(-1)).toEqual([{ id: ["session-1", 1], body: "First" }])
	subscription.destroy()
})

test("transaction queries include staged records and relations", async () => {
	const { server } = createServer()
	const tx = server.transact()

	tx.set("users", { id: "user-1", name: "Ada" })
	tx.set("threads", {
		id: "thread-1",
		ownerId: "user-1",
		title: "Draft",
		status: "open",
		createdAt: 1,
	})

	const result = await tx.query({
		collection: "threads",
		select: { title: true },
		with: { owner: { select: { name: true } } },
	})
	expect(result).toEqual([{ title: "Draft", owner: { name: "Ada" } }])
	expect(await server.query({ collection: "threads" })).toEqual([])
	await tx.cancel()
	await server.close()
})

test("commit rejects on storage and concurrent read/write conflicts", async () => {
	const { server, storage } = createServer()
	await seed(server)

	const reader = server.transact()
	await reader.get("threads", "thread-1")

	const writer = server.transact()
	writer.set("threads", {
		id: "thread-1",
		ownerId: "user-1",
		title: "Writer",
		status: "open",
		createdAt: 1,
	})
	await server.commit(writer)

	reader.set("threads", {
		id: "thread-1",
		ownerId: "user-1",
		title: "Reader",
		status: "open",
		createdAt: 1,
	})
	await expect(server.commit(reader)).rejects.toThrow(
		"Tandem server commit failed",
	)

	const failing = server.transact()
	failing.set("users", { id: "user-2", name: "Grace" })
	const storageCause = new Error("storage unavailable")
	storage.failNextCommit(storageCause)
	await expect(server.commit(failing)).rejects.toMatchObject({
		cause: storageCause,
	})
})

test("query supports scalar options and nested relations", async () => {
	const { server } = createServer()
	await seed(server)

	const page = await server.query({
		collection: "threads",
		where: { status: "open" },
		orderBy: { createdAt: "desc" },
		offset: 1,
		limit: 1,
		select: { id: true, title: true },
	})
	expect(page).toEqual([{ id: "thread-1", title: "Alpha" }])
	expectTypeOf(page).toMatchTypeOf<{ id: string; title: string }[]>()

	const nested = await server.query({
		collection: "users",
		select: { name: true },
		with: {
			threads: {
				where: { status: "open" },
				select: { title: true },
				orderBy: { createdAt: "asc" },
				with: {
					messages: {
						select: { body: true },
						orderBy: { createdAt: "desc" },
					},
				},
			},
		},
	})
	expect(nested).toEqual([
		{
			name: "Ada",
			threads: [
				{
					title: "Alpha",
					messages: [{ body: "Latest" }, { body: "First" }],
				},
				{ title: "Bravo", messages: [] },
			],
		},
	])

	const withOwner = await server.query({
		collection: "threads",
		where: { id: "thread-1" },
		select: { id: true },
		with: { owner: { select: { name: true } } },
	})
	expect(withOwner).toEqual([{ id: "thread-1", owner: { name: "Ada" } }])
})

test("subscriptions recompute for root and included records until destroyed", async () => {
	const { server } = createServer()
	await seed(server)
	const results: unknown[] = []
	const subscription = await server.subscribe(
		{
			collection: "threads",
			where: { id: "thread-1" },
			select: { title: true },
			with: { messages: { select: { body: true } } },
		},
		(result) => results.push(result),
	)

	expect(subscription.result).toEqual([
		{
			title: "Alpha",
			messages: [{ body: "First" }, { body: "Latest" }],
		},
	])

	const rootUpdate = server.transact()
	await rootUpdate.update("threads", "thread-1", (thread) => ({
		...thread,
		title: "Updated",
	}))
	await server.commit(rootUpdate)
	expect(results.at(-1)).toMatchObject([{ title: "Updated" }])

	const includedUpdate = server.transact()
	await includedUpdate.update("messages", "message-1", (message) => ({
		...message,
		body: "Edited",
	}))
	await server.commit(includedUpdate)
	expect(results.at(-1)).toMatchObject([
		{ messages: [{ body: "Edited" }, { body: "Latest" }] },
	])

	const resultCount = results.length
	subscription.destroy()
	const afterDestroy = server.transact()
	await afterDestroy.update("threads", "thread-1", (thread) => ({
		...thread,
		title: "Ignored",
	}))
	await server.commit(afterDestroy)
	expect(results).toHaveLength(resultCount)
})

test("subscriptions reject initial reads and report recomputation failures", async () => {
	const { server, storage } = createServer()
	await seed(server)
	storage.failNextScan(new Error("initial read failed"))
	const initialCallbacks: unknown[] = []
	await expect(
		server.subscribe({ collection: "threads" }, (result) =>
			initialCallbacks.push(result),
		),
	).rejects.toThrow("Tandem server subscribe failed")

	const callbacks: unknown[] = []
	const errors: Error[] = []
	const subscription = await server.subscribe(
		{ collection: "threads" },
		(result) => callbacks.push(result),
		{ onError: (error) => errors.push(error) },
	)
	storage.failNextScan(new Error("recompute failed"))
	const update = server.transact()
	update.set("threads", {
		id: "thread-1",
		ownerId: "user-1",
		title: "Triggers recompute",
		status: "open",
		createdAt: 1,
	})
	await server.commit(update)
	expect(errors.at(-1)).toBeInstanceOf(Error)

	const callbackCount = callbacks.length
	await server.close()
	expect(storage.closed).toBe(true)
	const afterClose = server.transact()
	afterClose.set("users", { id: "user-2", name: "Grace" })
	await server.commit(afterClose)
	subscription.destroy()
	expect(callbacks).toHaveLength(callbackCount)
	expect(initialCallbacks).toEqual([])
})

test("remote pushes preserve operation order and acknowledge the last mutation on every pull", async () => {
	const { server } = createServer()
	const clientId = tag<ClientId>("offline-client")
	const scanWindow: ScanWindow<TestSchema> = [{ collection: "threads" }]
	const initial = await server.pull({ clientId, scanWindow })
	const mutations: Parameters<RemoteApi<TestSchema>["push"]>[0]["mutations"] = [
		{
			id: tag<MutationId>(1),
			ops: [
				{
					type: "set",
					collection: "threads",
					value: {
						id: "thread-1",
						ownerId: "user-1",
						title: "First",
						status: "open",
						createdAt: 1,
					},
				},
			],
		},
		{
			id: tag<MutationId>(2),
			ops: [
				{
					type: "set",
					collection: "threads",
					value: {
						id: "thread-1",
						ownerId: "user-1",
						title: "Last write wins",
						status: "open",
						createdAt: 2,
					},
				},
				{
					type: "set",
					collection: "threads",
					value: {
						id: "thread-2",
						ownerId: "user-1",
						title: "Removed in batch",
						status: "open",
						createdAt: 3,
					},
				},
				{ type: "remove", collection: "threads", id: "thread-2" },
			],
		},
	]

	// Push and pull work without a prior connect registration.
	assert(!("error" in initial))
	expect(initial.patch.reset).toBe(true)
	await server.push({ clientId, mutations })
	const acknowledged = await server.pull({
		clientId,
		cookie: initial.cookie,
		scanWindow,
	})
	assert(!("error" in acknowledged))
	expect(acknowledged.lastMutationId).toBe(2)
	expect(acknowledged.cookie).not.toBe(initial.cookie)
	expect(acknowledged.patch.set).toEqual([
		{
			collection: "threads",
			value: {
				id: "thread-1",
				ownerId: "user-1",
				title: "Last write wins",
				status: "open",
				createdAt: 2,
			},
		},
	])

	// Unchanged pulls resend current records and still carry the acknowledgement.
	const unchanged = await server.pull({
		clientId,
		cookie: acknowledged.cookie,
		scanWindow,
	})
	assert(!("error" in unchanged))
	expect(unchanged.cookie).not.toBe(acknowledged.cookie)
	expect(unchanged).toMatchObject({
		patch: {
			set: [
				{
					collection: "threads",
					value: {
						id: "thread-1",
						ownerId: "user-1",
						title: "Last write wins",
						status: "open",
						createdAt: 2,
					},
				},
			],
			remove: [],
		},
		lastMutationId: 2,
	})

	await server.close()
})

test("concurrent duplicate pushes are skipped and pulls wait for committed effects", async () => {
	const { server, storage } = createServer()
	const clientId = tag<ClientId>("concurrent-client")
	const scanWindow: ScanWindow<TestSchema> = [{ collection: "users" }]
	const mutations: Mutation<TestSchema>[] = [
		{
			id: tag<MutationId>(1),
			ops: [
				{
					type: "set",
					collection: "users",
					value: { id: "user-1", name: "Ada" },
				},
			],
		},
	]
	let pokes = 0
	await server.connect({
		clientId,
		poke: () => {
			pokes += 1
			return Promise.resolve()
		},
	})
	const gate = storage.pauseNextCommit()
	const first = server.push({ clientId, mutations })
	await gate.entered
	const duplicate = server.push({ clientId, mutations })
	const pull = server.pull({ clientId, scanWindow })
	const edit = server.transact()
	edit.set("users", { id: "user-1", name: "Grace" })
	const commit = server.commit(edit)
	gate.release()
	await Promise.all([first, duplicate, commit])

	expect(await pull).toMatchObject({
		lastMutationId: 1,
		patch: {
			set: [{ collection: "users", value: { id: "user-1", name: "Ada" } }],
		},
	})
	expect(await server.query({ collection: "users" })).toEqual([
		{ id: "user-1", name: "Grace" },
	])
	expect(pokes).toBe(2)
	await server.close()
})

test("retrying a partially committed batch skips its prefix and processes its suffix", async () => {
	const { server, storage } = createServer()
	const clientId = tag<ClientId>("batch-client")
	const scanWindow: ScanWindow<TestSchema> = [{ collection: "users" }]
	const mutations: Mutation<TestSchema>[] = [
		{
			id: tag<MutationId>(1),
			ops: [
				{
					type: "set",
					collection: "users",
					value: { id: "user-1", name: "Ada" },
				},
			],
		},
		{
			id: tag<MutationId>(2),
			ops: [
				{
					type: "set",
					collection: "users",
					value: { id: "user-2", name: "Grace" },
				},
			],
		},
	]
	const storageCause = new Error("second mutation fails")
	const disconnect = await server.connect({
		clientId,
		poke: () => {
			storage.failNextCommit(storageCause)
			return Promise.resolve()
		},
	})
	await expect(server.push({ clientId, mutations })).resolves.toMatchObject({
		error: "unavailable",
	})
	await disconnect()
	expect(await server.pull({ clientId, scanWindow })).toMatchObject({
		lastMutationId: 1,
		patch: {
			set: [{ collection: "users", value: { id: "user-1", name: "Ada" } }],
		},
	})

	const edit = server.transact()
	edit.set("users", { id: "user-1", name: "Newer edit" })
	await server.commit(edit)
	await server.push({ clientId, mutations })
	expect(await server.pull({ clientId, scanWindow })).toMatchObject({
		lastMutationId: 2,
	})
	expect(await server.query({ collection: "users" })).toEqual([
		{ id: "user-1", name: "Newer edit" },
		{ id: "user-2", name: "Grace" },
	])
	await server.close()
})

test("a gap preserves the committed prefix and a consecutive retry recovers", async () => {
	const { server } = createServer()
	const clientId = tag<ClientId>("gap-client")
	const mutations: Mutation<TestSchema>[] = [1, 2, 3].map((id) => ({
		id: tag<MutationId>(id),
		ops: [
			{
				type: "set",
				collection: "users",
				value: { id: `user-${id}`, name: `User ${id}` },
			},
		],
	}))
	const scanWindow: ScanWindow<TestSchema> = [{ collection: "users" }]

	await expect(
		server.push({
			clientId,
			mutations: [mutations[0]!, mutations[2]!],
		}),
	).resolves.toEqual({
		error: "mutation-gap",
		expectedMutationId: 2,
		receivedMutationId: 3,
	})
	expect(await server.pull({ clientId, scanWindow })).toMatchObject({
		lastMutationId: 1,
	})
	expect(await server.query({ collection: "users" })).toEqual([
		{ id: "user-1", name: "User 1" },
	])

	// Skip the already committed ID 1, then accept IDs 2 and 3 in order.
	await server.push({ clientId, mutations })
	expect(await server.pull({ clientId, scanWindow })).toMatchObject({
		lastMutationId: 3,
	})
	expect(await server.query({ collection: "users" })).toEqual([
		{ id: "user-1", name: "User 1" },
		{ id: "user-2", name: "User 2" },
		{ id: "user-3", name: "User 3" },
	])
	await server.close()
})

test("empty mutations advance acknowledgement once, without re-poking on retry", async () => {
	const { server } = createServer()
	const clientId = tag<ClientId>("empty-client")
	let pokes = 0
	await server.connect({
		clientId,
		poke: () => {
			pokes += 1
			return Promise.resolve()
		},
	})
	const mutations: Mutation<TestSchema>[] = [
		{ id: tag<MutationId>(1), ops: [] },
	]
	await server.push({ clientId, mutations })
	const confirmed = await server.pull({ clientId, scanWindow: [] })
	assert(!("error" in confirmed))
	await server.push({ clientId, mutations })
	expect(
		await server.pull({ clientId, scanWindow: [], cookie: confirmed.cookie }),
	).toMatchObject({
		lastMutationId: 1,
		patch: { set: [], remove: [] },
	})
	expect(pokes).toBe(1)
	await server.close()
})

test("push acknowledgement is visible to the pull started by its poke", async () => {
	const { server } = createServer()
	const clientId = tag<ClientId>("poked-client")
	const mutationId = tag<MutationId>(1)
	const scanWindow: ScanWindow<TestSchema> = [{ collection: "users" }]
	const initial = await server.pull({ clientId, scanWindow })
	assert(!("error" in initial))
	const pokedPull =
		Promise.withResolvers<Awaited<ReturnType<RemoteApi<TestSchema>["pull"]>>>()
	const disconnect = await server.connect({
		clientId,
		poke: () =>
			server
				.pull({ clientId, cookie: initial.cookie, scanWindow })
				.then(pokedPull.resolve, pokedPull.reject),
	})

	await server.push({
		clientId,
		mutations: [
			{
				id: mutationId,
				ops: [
					{
						type: "set",
						collection: "users",
						value: { id: "user-1", name: "Ada" },
					},
				],
			},
		],
	})

	const result = await pokedPull.promise
	assert(!("error" in result))
	expect(result.lastMutationId).toBe(mutationId)
	expect(result.patch.set).toEqual([
		{ collection: "users", value: { id: "user-1", name: "Ada" } },
	])

	await disconnect()
	await server.close()
})

test("pull diffs filtered, paginated, and nested client views", async () => {
	const { server } = createServer()
	await seed(server)
	const messageTx = server.transact()
	messageTx.set("messages", {
		id: "thread-3",
		threadId: "thread-3",
		body: "Top thread message",
		createdAt: 3,
	})
	await server.commit(messageTx)

	const clientId = tag<ClientId>("view-client")
	const nestedPage: ScanWindow<TestSchema> = [
		{
			collection: "threads",
			select: ["id"],
			where: [["status", "=", "open"]],
			order: [["createdAt", "desc"]],
			limit: 1,
			with: {
				owner: { collection: "users", select: ["id"] },
				messages: {
					collection: "messages",
					select: ["id"],
					order: [["createdAt", "desc"]],
					limit: 1,
				},
			},
		},
	]

	// The initial page includes complete root and related records.
	const initial = await server.pull({ clientId, scanWindow: nestedPage })
	assert(!("error" in initial))
	expect(patchSetKeys(initial.patch)).toEqual([
		"messages.thread-3",
		"threads.thread-3",
		"users.user-1",
	])
	expect(initial.patch.set).toContainEqual({
		collection: "threads",
		value: {
			id: "thread-3",
			ownerId: "user-1",
			title: "Bravo",
			status: "open",
			createdAt: 3,
		},
	})

	// A filtered row leaving the page removes it and promotes the next row.
	const closeTopTx = server.transact()
	await closeTopTx.update("threads", "thread-3", (thread) => ({
		...thread,
		status: "closed",
	}))
	await server.commit(closeTopTx)
	const promoted = await server.pull({
		clientId,
		cookie: initial.cookie,
		scanWindow: nestedPage,
	})
	assert(!("error" in promoted))
	expect(patchSetKeys(promoted.patch)).toEqual([
		"messages.message-2",
		"threads.thread-1",
		"users.user-1",
	])
	expect(patchRemoveKeys(promoted.patch)).toEqual([
		"messages.thread-3",
		"threads.thread-3",
	])

	// A row entering above the page displaces the previous page member.
	const openNewestTx = server.transact()
	await openNewestTx.update("threads", "thread-2", (thread) => ({
		...thread,
		status: "open",
		createdAt: 4,
	}))
	await server.commit(openNewestTx)
	const displaced = await server.pull({
		clientId,
		cookie: promoted.cookie,
		scanWindow: nestedPage,
	})
	assert(!("error" in displaced))
	expect(patchSetKeys(displaced.patch)).toEqual([
		"threads.thread-2",
		"users.user-1",
	])
	expect(patchRemoveKeys(displaced.patch)).toEqual([
		"messages.message-2",
		"threads.thread-1",
	])

	// Shrinking the scan window removes records that were included only by it.
	const rootOnlyPage: ScanWindow<TestSchema> = [
		{
			collection: "threads",
			where: [["status", "=", "open"]],
			order: [["createdAt", "desc"]],
			limit: 1,
		},
	]
	const shrunk = await server.pull({
		clientId,
		cookie: displaced.cookie,
		scanWindow: rootOnlyPage,
	})
	assert(!("error" in shrunk))
	expect(patchSetKeys(shrunk.patch)).toEqual(["threads.thread-2"])
	expect(patchRemoveKeys(shrunk.patch)).toEqual(["users.user-1"])

	// An unchanged cookie and scan window still read and resend the current page.
	const unchanged = await server.pull({
		clientId,
		cookie: shrunk.cookie,
		scanWindow: rootOnlyPage,
	})
	assert(!("error" in unchanged))
	expect(patchSetKeys(unchanged.patch)).toEqual(["threads.thread-2"])
	expect(unchanged.patch.remove).toEqual([])
	expect(unchanged.cookie).not.toBe(shrunk.cookie)

	await server.close()
})

test("pull returns an invalid-request error for a relation targeting the wrong collection", async () => {
	const { server } = createServer()
	const invalidScanWindow: ScanWindow<TestSchema> = [
		{
			collection: "threads",
			with: {
				owner: { collection: "messages" },
			},
		},
	]

	await expect(
		server.pull({
			clientId: tag<ClientId>("invalid-query-client"),
			scanWindow: invalidScanWindow,
		}),
	).resolves.toEqual({
		error: "invalid-request",
		message:
			'Relation "threads.owner" targets collection "users", not "messages"',
	})

	await server.close()
})

test("pull returns unavailable on a storage failure and a later pull recovers", async () => {
	const { server, storage } = createServer()
	await seed(server)
	const args = {
		clientId: tag<ClientId>("read-failure"),
		scanWindow: [{ collection: "users" as const }],
	}
	storage.failNextScan(new Error("Storage disconnected"))
	await expect(server.pull(args)).resolves.toMatchObject({
		error: "unavailable",
	})
	await expect(server.pull(args)).resolves.toMatchObject({
		lastMutationId: 0,
		patch: {
			set: [{ collection: "users", value: { id: "user-1", name: "Ada" } }],
		},
	})
	await server.close()
})

test("storage notifications preserve deletion history and release their listener on close", async () => {
	const storage = new TestTandemServerStorage<TestSchema>()
	const listeners = new Set<() => void>()
	const server = new TandemServer({
		schema,
		relations,
		storage: {
			scan: (args) => storage.scan(args),
			commit: (writes) => storage.commit(writes),
			close: () => storage.close(),
			subscribe: (onChange) => {
				listeners.add(onChange)
				return () => {
					// A notification already queued by the backend may arrive during teardown.
					onChange()
					listeners.delete(onChange)
				}
			},
		},
	})
	const clientId = tag<ClientId>("storage-notifications-client")
	let pokes = 0
	await server.connect({
		clientId,
		poke: () => {
			pokes += 1
			return Promise.resolve()
		},
	})
	await server.connect({
		clientId: tag<ClientId>("second-storage-notifications-client"),
		poke: () => Promise.resolve(),
	})
	expect(listeners.size).toBe(1)
	const onChange = [...listeners][0]!
	await storage.commit({
		set: [
			{
				key: ["record", "users", "user-1"],
				value: { id: "user-1", name: "Ada" },
			},
		],
	})
	const scanWindow: ScanWindow<TestSchema> = [{ collection: "users" }]
	const initial = await server.pull({ clientId, scanWindow })
	assert(!("error" in initial))

	// Duplicate notifications request pulls without discarding the prior keys or cookie.
	await storage.commit({ remove: [["record", "users", "user-1"]] })
	onChange()
	onChange()
	expect(pokes).toBe(2)
	const changed = await server.pull({
		clientId,
		scanWindow,
		cookie: initial.cookie,
	})
	assert(!("error" in changed))
	expect(changed.patch).toEqual({
		set: [],
		remove: [{ collection: "users", id: "user-1" }],
	})

	// Close detaches the backend listener and ignores notifications once teardown starts.
	await server.close()
	expect(listeners.size).toBe(0)
	expect(storage.closed).toBe(true)
	onChange()
	expect(pokes).toBe(2)
})

test("application commits poke connected clients until they disconnect or close", async () => {
	const { server, storage } = createServer()
	const firstPoke = vi.fn()
	const secondPoke = vi.fn()
	const firstClientId = tag<ClientId>("connected-1")
	const secondClientId = tag<ClientId>("connected-2")
	const disconnectFirst = await server.connect({
		clientId: firstClientId,
		poke: firstPoke,
	})
	const disconnectSecond = await server.connect({
		clientId: secondClientId,
		poke: secondPoke,
	})

	// Empty pushes and application transactions do not advance or poke.
	await server.push({ clientId: firstClientId, mutations: [] })
	await server.commit(server.transact())
	expect(firstPoke).not.toHaveBeenCalled()
	expect(secondPoke).not.toHaveBeenCalled()

	// A successful application commit conservatively pokes every connection.
	const firstTx = server.transact()
	firstTx.set("users", { id: "user-1", name: "Ada" })
	await server.commit(firstTx)
	expect(firstPoke).toHaveBeenCalledTimes(1)
	expect(secondPoke).toHaveBeenCalledTimes(1)

	// Disconnect is idempotent and only detaches that client's callback.
	await disconnectFirst()
	await disconnectFirst()
	const secondTx = server.transact()
	secondTx.set("users", { id: "user-2", name: "Grace" })
	await server.commit(secondTx)
	expect(firstPoke).toHaveBeenCalledTimes(1)
	expect(secondPoke).toHaveBeenCalledTimes(2)

	// Close clears remaining sync registrations before closing storage.
	await server.close()
	expect(storage.closed).toBe(true)
	const afterCloseTx = server.transact()
	afterCloseTx.set("users", { id: "user-3", name: "Katherine" })
	await server.commit(afterCloseTx)
	expect(secondPoke).toHaveBeenCalledTimes(2)
	await disconnectSecond()
})

test("failed pushes do not acknowledge, advance, or poke", async () => {
	const { server, storage } = createServer()
	const clientId = tag<ClientId>("failing-client")
	const poke = vi.fn()
	await server.connect({ clientId, poke })
	const scanWindow: ScanWindow<TestSchema> = [{ collection: "users" }]
	const initial = await server.pull({ clientId, scanWindow })
	const storageCause = new Error("push storage unavailable")
	storage.failNextCommit(storageCause)

	await expect(
		server.push({
			clientId,
			mutations: [
				{
					id: tag<MutationId>(1),
					ops: [
						{
							type: "set",
							collection: "users",
							value: { id: "user-1", name: "Ada" },
						},
					],
				},
			],
		}),
	).resolves.toMatchObject({ error: "unavailable" })
	expect(poke).not.toHaveBeenCalled()

	assert(!("error" in initial))
	const afterFailure = await server.pull({
		clientId,
		cookie: initial.cookie,
		scanWindow,
	})
	expect(afterFailure).toMatchObject({
		patch: { set: [], remove: [] },
		lastMutationId: 0,
	})
	expect(await server.query({ collection: "users" })).toEqual([])

	await server.close()
})
