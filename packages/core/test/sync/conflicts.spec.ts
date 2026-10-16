import {
	type LoggerApi,
	type RemoteApi,
	TandemClient,
} from "@tanishqkancharla/tandem-core"
import * as errore from "errore"
import { describe, expect, vi } from "vitest"
import {
	buildGatekeeperHarness,
	expectQuery,
	test,
	todo,
	type DemoRng,
	type TestsSchema,
	type TestsTodo,
} from "../fixtures.js"

function createTodoGatekeeper({
	remote,
	logger,
	rng,
}: {
	remote: RemoteApi<TestsSchema>
	logger: LoggerApi
	rng: DemoRng
}) {
	const clients: TandemClient<TestsSchema>[] = []
	const gatekeeper = buildGatekeeperHarness({
		server: remote,
		createClient: (server, label) => {
			const client = new TandemClient<TestsSchema>({
				remote: server,
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

type ClientName = "client1" | "client2"
type TodoGatekeeper = ReturnType<typeof createTodoGatekeeper>["gatekeeper"]
const conflictTest = test.extend<{
	makeTodoGatekeeper: (args: {
		remote: RemoteApi<TestsSchema>
		connect?: readonly ClientName[]
	}) => Promise<TodoGatekeeper>
}>({
	makeTodoGatekeeper: async ({ logger, rng }, use) => {
		await using cleanup = new errore.AsyncDisposableStack()

		await use(async ({ remote, connect = ["client1", "client2"] }) => {
			const { clients, gatekeeper } = createTodoGatekeeper({
				remote,
				logger,
				rng,
			})
			const connectedClients = clients.filter((_, index) =>
				connect.includes(index === 0 ? "client1" : "client2"),
			)
			cleanup.defer(async () => {
				await gatekeeper.deactivateGatesAndSettle()
				await Promise.all(clients.map((client) => client.disconnect()))
				await gatekeeper[Symbol.asyncDispose]()
			})

			await Promise.all(clients.map((client) => client.ready))
			await Promise.all(connectedClients.map((client) => client.connect()))
			return gatekeeper
		})
	},
})

describe("TandemClient sync conflicts", () => {
	conflictTest(
		"pull writes invalidate drafts even when replay preserves the optimistic record",
		async ({ makeTodoGatekeeper, server }) => {
			const remote: RemoteApi<TestsSchema> = {
				connect: (client) =>
					server.connect({ ...client, poke: async () => {} }),
				pull: (args) => server.pull(args),
				push: (args) => server.push(args),
			}
			const gatekeeper = await makeTodoGatekeeper({ remote })
			const { client1 } = gatekeeper
			client1.subscribe({ collection: "todos" })
			const seedTx = server.transact()
			seedTx.set("todos", todo("shared", { text: "Original" }))
			await server.commit(seedTx)
			await (
				await client1.pullFromRemote()
			).result

			const pendingTx = client1.transact()
			pendingTx.set("todos", todo("shared", { text: "Pending" }))
			await gatekeeper.activateGates()
			const pending = await client1.commit(pendingTx)
			await pending.fail(new Error("Lost request"))
			await expect(pending.result).rejects.toThrow("Lost request")
			await gatekeeper.deactivateGatesAndSettle()

			// A pull conflicts with the draft even when replay preserves the value it read.
			const draftTx = client1.transact()
			draftTx.update("todos", "shared", (record) => ({
				...record,
				priority: 9,
			}))
			draftTx.set("todos", todo("ghost", { text: "Rejected draft" }))
			const remoteTx = server.transact()
			remoteTx.set(
				"todos",
				todo("shared", { text: "New remote base", priority: 3 }),
			)
			await server.commit(remoteTx)
			await (
				await client1.pullFromRemote()
			).result
			expect(client1.query({ collection: "todos" })).toEqual([
				todo("shared", { text: "Pending" }),
			])
			expect(() => client1.commit(draftTx)).toThrow("ReadWriteConflictError")
			expect(client1.query({ collection: "todos" })).toEqual([
				todo("shared", { text: "Pending" }),
			])

			// A fresh transaction sends both pending edits without a ghost write or mutation-ID gap.
			const retryTx = client1.transact()
			retryTx.update("todos", "shared", (record) => ({
				...record,
				priority: 9,
			}))
			await (
				await client1.commit(retryTx)
			).result
			await (
				await client1.pullFromRemote()
			).result

			const expected = [todo("shared", { text: "Pending", priority: 9 })]
			expect(client1.query({ collection: "todos" })).toEqual(expected)
			expect(await server.query({ collection: "todos" })).toEqual(expected)
		},
	)

	conflictTest(
		"a rejected draft cannot leave pending writes or consume a mutation ID",
		async ({ makeTodoGatekeeper, server }) => {
			const remote: RemoteApi<TestsSchema> = {
				connect: (client) =>
					server.connect({ ...client, poke: async () => {} }),
				pull: (args) => server.pull(args),
				push: (args) => server.push(args),
			}
			const gatekeeper = await makeTodoGatekeeper({ remote })
			const { client1 } = gatekeeper
			client1.subscribe({ collection: "todos" })
			const seedTx = server.transact()
			seedTx.set("todos", todo("shared"))
			await server.commit(seedTx)
			await (
				await client1.pullFromRemote()
			).result

			// Another commit invalidates the draft before any pending state is published.
			const staleTx = client1.transact()
			staleTx.get("todos", "shared")
			staleTx.set("todos", todo("shared", { text: "Rejected" }))
			staleTx.set("todos", todo("ghost"))
			const winner = todo("shared", { text: "Winner" })
			const winnerTx = client1.transact()
			winnerTx.set("todos", winner)
			await (
				await client1.commit(winnerTx)
			).result
			expect(() => client1.commit(staleTx)).toThrow("ReadWriteConflictError")
			expect(client1.query({ collection: "todos" })).toEqual([winner])

			// The next valid write must deliver without a gap or a replayed ghost.
			const next = todo("next", { text: "Valid next write" })
			const nextTx = client1.transact()
			nextTx.set("todos", next)
			await (
				await client1.commit(nextTx)
			).result
			await (
				await client1.pullFromRemote()
			).result
			expect(await server.query({ collection: "todos" })).toEqual([
				next,
				winner,
			])
			expect(client1.query({ collection: "todos" })).toEqual([next, winner])
		},
	)

	conflictTest(
		"a lost removal response recovers without losing pending writes",
		async ({ makeTodoGatekeeper, server }) => {
			let loseResponse = false
			const remote: RemoteApi<TestsSchema> = {
				connect: (client) =>
					server.connect({ ...client, poke: async () => {} }),
				push: (args) => server.push(args),
				pull: async (args) => {
					const response = await server.pull(args)
					if (loseResponse) {
						loseResponse = false
						return { error: "unavailable", message: "Lost response" }
					}
					return response
				},
			}
			const gatekeeper = await makeTodoGatekeeper({ remote })
			const { client1 } = gatekeeper
			const subscription = client1.subscribe({ collection: "todos" })
			const seedTx = server.transact()
			seedTx.set("todos", todo("ghost"))
			seedTx.set("todos", todo("edited"))
			await server.commit(seedTx)
			await (
				await client1.pullFromRemote()
			).result

			// Keep a local edit pending while the server deletes both old records.
			await gatekeeper.activateGates()
			const editTx = client1.transact()
			const edited = todo("edited", { text: "Pending edit" })
			editTx.set("todos", edited)
			const edit = await client1.commit(editTx)
			await edit.fail(new Error("Lost request"))
			await expect(edit.result).rejects.toThrow("Lost request")
			await gatekeeper.deactivateGatesAndSettle()
			const deleteTx = server.transact()
			deleteTx.remove("todos", "ghost")
			deleteTx.remove("todos", "edited")
			await server.commit(deleteTx)
			loseResponse = true
			await expect((await client1.pullFromRemote()).result).rejects.toThrow()

			// Retrying the old cookie resets the replica but replays the pending edit.
			await (
				await client1.pullFromRemote()
			).result
			expect(client1.query({ collection: "todos" })).toEqual([edited])

			// Confirm outside the subscribed window: the old base must not reappear.
			subscription.destroy()
			await (
				await client1.disconnect()
			).result
			await (
				await client1.connect()
			).result
			await (
				await client1.pullFromRemote()
			).result
			expect(await server.query({ collection: "todos" })).toEqual([edited])
			expect(client1.query({ collection: "todos" })).toEqual([])
		},
	)

	test("retains a lost push and delivers it with the next write", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		client2.subscribe({ collection: "todos" })
		const offlineDraft = todo("offline-todo", {
			text: "Write while offline",
			priority: 1,
		})
		let latestResult: TestsTodo[] | undefined
		client1.subscribe({ collection: "todos" }, (result) => {
			latestResult = result
		})
		await gatekeeper.activateGates()

		const tx = client1.transact()
		tx.set("todos", offlineDraft)
		const commit = await client1.commit(tx)

		expect(client1.query({ collection: "todos" })).toEqual([offlineDraft])
		expect(latestResult).toEqual([offlineDraft])

		const failure = new Error("offline")
		await commit.fail(failure)

		await expect(commit.result).rejects.toBe(failure)
		expect(latestResult).toEqual([offlineDraft])
		expect(client1.query({ collection: "todos" })).toEqual([offlineDraft])

		// The next push carries both writes, with no gap left by the failed attempt.
		const nextTodo = todo("second-todo", { text: "Next write" })
		const nextTx = client1.transact()
		nextTx.set("todos", nextTodo)
		const nextCommit = await client1.commit(nextTx)
		await nextCommit.continueToCompletion()
		const pull = await client2.pullFromRemote()
		await pull.continueToCompletion()
		expect(client2.query({ collection: "todos" })).toEqual([
			offlineDraft,
			nextTodo,
		])
	})

	test("clear discards offline writes and lets the next session sync", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		client1.subscribe({ collection: "todos" })
		client2.subscribe({ collection: "todos" })
		await (
			await client1.disconnect()
		).result
		const oldTx = client1.transact()
		oldTx.set("todos", todo("discarded"))
		await (
			await client1.commit(oldTx)
		).result

		await (
			await client1.clear()
		).result
		expect(client1.query({ collection: "todos" })).toEqual([])

		// A fresh write must not wait for the discarded mutation's ID.
		const nextTodo = todo("kept", { text: "After clear" })
		const nextTx = client1.transact()
		nextTx.set("todos", nextTodo)
		await (
			await client1.commit(nextTx)
		).result
		await (
			await client1.connect()
		).result
		await expectQuery(client2, { collection: "todos" }).toResolveTo([nextTodo])
		await expectQuery(client1, { collection: "todos" }).toResolveTo([nextTodo])
	})

	test("clear ignores an old pull response without dropping new-session writes", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		client1.subscribe({ collection: "todos" })
		client2.subscribe({ collection: "todos" })
		const oldTodo = todo("old", { text: "Already delivered" })
		const oldTx = client1.transact()
		oldTx.set("todos", oldTodo)
		await (
			await client1.commit(oldTx)
		).result
		await (
			await client1.pullFromRemote()
		).result
		await gatekeeper.activateGates()
		const oldPull = await client1.pullFromRemote()
		await oldPull.continueTo("server")

		const clear = await client1.clear()
		await clear.continueToCompletion()
		expect(client1.query({ collection: "todos" })).toEqual([])

		// The old acknowledgement of ID 1 must not prune the new session's ID 1.
		const nextTodo = todo("new", { text: "New session" })
		const nextTx = client1.transact()
		nextTx.set("todos", nextTodo)
		const nextReady = client1.commit(nextTx)
		await oldPull.continueToCompletion()
		expect(client1.query({ collection: "todos" })).toEqual([nextTodo])
		const next = await nextReady
		await next.continueToCompletion()
		const pull = await client2.pullFromRemote()
		await pull.continueToCompletion()
		expect(client2.query({ collection: "todos" })).toEqual([nextTodo, oldTodo])
	})

	conflictTest(
		"an empty pull acknowledges only the confirmed prefix of the next push",
		async ({ makeTodoGatekeeper, server }) => {
			const deliveries: number[][] = []
			const remote: RemoteApi<TestsSchema> = {
				connect: (client) =>
					server.connect({ ...client, poke: async () => {} }),
				push: (args) => {
					deliveries.push(args.mutations.map(({ id }) => id))
					return server.push(args)
				},
				pull: (args) => server.pull(args),
			}
			const gatekeeper = await makeTodoGatekeeper({ remote })
			const { client1, client2 } = gatekeeper
			client2.subscribe({ collection: "todos" })
			const firstTx = client1.transact()
			firstTx.set("todos", todo("confirmed"))
			await (
				await client1.commit(firstTx)
			).result

			// Success is not confirmation: the first mutation is sent again.
			const secondTodo = todo("pending")
			const secondTx = client1.transact()
			secondTx.set("todos", secondTodo)
			await gatekeeper.activateGates()
			const second = await client1.commit(secondTx)
			await second.continueTo("server")
			expect(deliveries).toEqual([[1], [1, 2]])
			await second.fail(new Error("Lost response"))
			await expect(second.result).rejects.toThrow("Lost response")
			expect(client1.query({ collection: "todos" })).toEqual([
				todo("confirmed"),
				secondTodo,
			])

			// A newer unsent mutation survives the empty-window acknowledgement of 1 and 2.
			const thirdTodo = todo("unsent")
			const thirdTx = client1.transact()
			thirdTx.set("todos", thirdTodo)
			const third = await client1.commit(thirdTx)
			await third.fail(new Error("Lost request"))
			await expect(third.result).rejects.toThrow("Lost request")
			const pull = await client1.pullFromRemote()
			await pull.continueToCompletion()
			expect(client1.query({ collection: "todos" })).toEqual([thirdTodo])

			const fourthTx = client1.transact()
			fourthTx.set("todos", todo("trigger"))
			const fourth = await client1.commit(fourthTx)
			await fourth.continueToCompletion()
			expect(deliveries).toEqual([[1], [1, 2], [3, 4]])
			const observerPull = await client2.pullFromRemote()
			await observerPull.continueToCompletion()
			expect(client2.query({ collection: "todos" })).toEqual([
				todo("confirmed"),
				secondTodo,
				todo("trigger"),
				thirdTodo,
			])
		},
	)

	conflictTest(
		"keeps pending mutations queued while disconnected and pushes them after reconnect",
		async ({ makeTodoGatekeeper, server }) => {
			const gatekeeper = await makeTodoGatekeeper({
				remote: server,
				connect: ["client2"],
			})
			const { client1, client2 } = gatekeeper
			client2.subscribe({ collection: "todos" })

			const tx = client1.transact()
			tx.set("todos", todo("todo-1", { text: "Write while disconnected" }))
			const commit = await client1.commit(tx)
			await commit.result
			const pullBeforeConnect = await client2.pullFromRemote()
			await pullBeforeConnect.result

			expect(client2.query({ collection: "todos" })).toEqual([])

			const connect = await client1.connect()
			await connect.result

			await expectQuery(client2, { collection: "todos" }).toResolveTo([
				todo("todo-1", { text: "Write while disconnected" }),
			])
		},
	)

	test("recovers local and remote changes after disconnecting and reconnecting", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		const client1Subscription = client1.subscribe({ collection: "todos" })
		const client2Subscription = client2.subscribe({ collection: "todos" })
		await (
			await client1.disconnect()
		).result

		// The connected client advances the server while client1 is offline
		const remoteTx = client2.transact()
		remoteTx.set("todos", todo("remote", { text: "Written remotely" }))
		await (
			await client2.commit(remoteTx)
		).result

		// Client1 keeps its local optimistic change queued while disconnected
		const localTx = client1.transact()
		localTx.set("todos", todo("local", { text: "Written offline" }))
		await (
			await client1.commit(localTx)
		).result

		expect(client1.query({ collection: "todos" })).toEqual([
			todo("local", { text: "Written offline" }),
		])

		// Reconnecting pulls the missed remote change and pushes the queued local change
		await (
			await client1.connect()
		).result

		const convergedTodos = [
			todo("local", { text: "Written offline" }),
			todo("remote", { text: "Written remotely" }),
		]
		await expectQuery(client1, { collection: "todos" }).toResolveTo(
			convergedTodos,
		)
		await expectQuery(client2, { collection: "todos" }).toResolveTo(
			convergedTodos,
		)
		client1Subscription.destroy()
		client2Subscription.destroy()
	})

	conflictTest(
		"recovers from a transient pull failure on the next pull",
		async ({ makeTodoGatekeeper, server }) => {
			let failingClientId = ""
			let shouldFail = false
			let failedPulls = 0
			const unreliableRemote: RemoteApi<TestsSchema> = {
				connect: (client) => server.connect(client),
				push: (args) => server.push(args),
				pull: (args) => {
					if (shouldFail && args.clientId === failingClientId) {
						shouldFail = false
						failedPulls += 1
						return Promise.resolve({
							error: "unavailable" as const,
							message: "Temporary pull failure",
						})
					}
					return server.pull(args)
				},
			}
			const gatekeeper = await makeTodoGatekeeper({ remote: unreliableRemote })
			const { client1, client2 } = gatekeeper
			failingClientId = client2.clientId
			const subscription = client2.subscribe({ collection: "todos" })
			await (
				await client2.pullFromRemote()
			).result
			shouldFail = true

			// The server poke reaches client2, but its first pull fails
			const tx = client1.transact()
			tx.set("todos", todo("todo-1", { text: "Retry this pull" }))
			await (
				await client1.commit(tx)
			).result
			await vi.waitFor(() => expect(failedPulls).toBe(1))

			expect(client2.query({ collection: "todos" })).toEqual([])

			// A later pull succeeds without reconstructing the client
			await (
				await client2.pullFromRemote()
			).result

			expect(client2.query({ collection: "todos" })).toEqual([
				todo("todo-1", { text: "Retry this pull" }),
			])
			subscription.destroy()
		},
	)

	conflictTest(
		"a returned connection error leaves offline writes available for reconnect",
		async ({ makeTodoGatekeeper, server }) => {
			let failConnect = false
			const remote: RemoteApi<TestsSchema> = {
				connect: (client) =>
					failConnect
						? Promise.resolve({
								error: "unavailable" as const,
								message: "Cannot connect",
							})
						: server.connect(client),
				pull: (args) => server.pull(args),
				push: (args) => server.push(args),
			}
			const { client1, client2 } = await makeTodoGatekeeper({
				remote,
				connect: ["client2"],
			})
			client2.subscribe({ collection: "todos" })
			const tx = client1.transact()
			tx.set("todos", todo("offline"))
			await (
				await client1.commit(tx)
			).result
			failConnect = true
			await expect((await client1.connect()).result).rejects.toMatchObject({
				cause: { error: "unavailable" },
			})
			expect(client1.query({ collection: "todos" })).toEqual([todo("offline")])

			failConnect = false
			await (
				await client1.connect()
			).result
			await expectQuery(client2, { collection: "todos" }).toResolveTo([
				todo("offline"),
			])
		},
	)

	conflictTest(
		"reports a returned push error without losing the write, then syncs on reconnect",
		async ({ makeTodoGatekeeper, server }) => {
			let failPush = true
			const remote: RemoteApi<TestsSchema> = {
				connect: (client) => server.connect(client),
				pull: (args) => server.pull(args),
				push: (args) => {
					if (failPush) {
						failPush = false
						return Promise.resolve({
							error: "unavailable" as const,
							message: "Storage unavailable",
						})
					}
					return server.push(args)
				},
			}
			const { client1, client2 } = await makeTodoGatekeeper({ remote })
			client1.subscribe({ collection: "todos" })
			client2.subscribe({ collection: "todos" })
			const draft = todo("retained")
			const tx = client1.transact()
			tx.set("todos", draft)
			const commit = await client1.commit(tx)
			await expect(commit.result).rejects.toMatchObject({
				cause: { error: "unavailable" },
			})
			expect(client1.query({ collection: "todos" })).toEqual([draft])

			// Reconnecting retries the retained write against the real server.
			await (
				await client1.disconnect()
			).result
			await (
				await client1.connect()
			).result
			await expectQuery(client2, { collection: "todos" }).toResolveTo([draft])
		},
	)

	conflictTest(
		"drops an acknowledged write that another client deleted while its pokes were lost",
		async ({ makeTodoGatekeeper, server }) => {
			let losePokesFor: string | undefined
			const remote: RemoteApi<TestsSchema> = {
				connect: (client) =>
					server.connect({
						...client,
						poke: () =>
							client.clientId === losePokesFor
								? Promise.resolve()
								: client.poke(),
					}),
				push: (args) => server.push(args),
				pull: (args) => server.pull(args),
			}
			const { client1, client2 } = await makeTodoGatekeeper({ remote })
			const client1Subscription = client1.subscribe({ collection: "todos" })
			const client2Subscription = client2.subscribe({ collection: "todos" })
			await (
				await client1.pullFromRemote()
			).result
			await (
				await client2.pullFromRemote()
			).result
			losePokesFor = client1.clientId

			// client1 writes a todo but never hears the poke for its own push
			const create = client1.transact()
			create.set("todos", todo("todo-1", { text: "Short-lived" }))
			await (
				await client1.commit(create)
			).result
			await expectQuery(client2, { collection: "todos" }).toResolveTo([
				todo("todo-1", { text: "Short-lived" }),
			])

			// client2 deletes it, leaving nothing in the window
			const remove = client2.transact()
			remove.remove("todos", "todo-1")
			await (
				await client2.commit(remove)
			).result

			// client1's next pull acknowledges its write against the empty window
			await (
				await client1.pullFromRemote()
			).result

			expect(client1.query({ collection: "todos" })).toEqual([])
			client1Subscription.destroy()
			client2Subscription.destroy()
		},
	)

	conflictTest(
		"keeps a record deleted after the writer removed it, before the writer read it back",
		async ({ makeTodoGatekeeper, server }) => {
			let losePokesFor: string | undefined
			const remote: RemoteApi<TestsSchema> = {
				connect: (client) =>
					server.connect({
						...client,
						poke: () =>
							client.clientId === losePokesFor
								? Promise.resolve()
								: client.poke(),
					}),
				push: (args) => server.push(args),
				pull: (args) => server.pull(args),
			}
			const { client1, client2 } = await makeTodoGatekeeper({ remote })
			const client1Subscription = client1.subscribe({ collection: "todos" })
			const client2Subscription = client2.subscribe({ collection: "todos" })

			// Another todo keeps later pulls from carrying an empty patch
			const seedTx = client1.transact()
			seedTx.set("todos", todo("todo-2", { text: "Keep me" }))
			await (
				await client1.commit(seedTx)
			).result
			await expectQuery(client2, { collection: "todos" }).toResolveTo([
				todo("todo-2", { text: "Keep me" }),
			])
			losePokesFor = client2.clientId

			// client2 creates todo-1 but never reads it back from the server
			const createTx = client2.transact()
			createTx.set("todos", todo("todo-1", { text: "Short-lived" }))
			await (
				await client2.commit(createTx)
			).result
			await expectQuery(client1, { collection: "todos" }).toResolveTo([
				todo("todo-1", { text: "Short-lived" }),
				todo("todo-2", { text: "Keep me" }),
			])

			// client1 deletes it on the server
			const deleteTx = client1.transact()
			deleteTx.remove("todos", "todo-1")
			await (
				await client1.commit(deleteTx)
			).result

			// Offline, client2 removes todo-1 too, while its create is unacknowledged
			await (
				await client2.disconnect()
			).result
			const removeTx = client2.transact()
			removeTx.remove("todos", "todo-1")
			await (
				await client2.commit(removeTx)
			).result

			// Reconnecting acknowledges the create; a later pull acknowledges the remove
			await (
				await client2.connect()
			).result
			await (
				await client2.pullFromRemote()
			).result

			expect(client2.query({ collection: "todos" })).toEqual([
				todo("todo-2", { text: "Keep me" }),
			])
			client1Subscription.destroy()
			client2Subscription.destroy()
		},
	)

	test("replays a pending mutation's operations in order across a pull", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		const client1Subscription = client1.subscribe({ collection: "todos" })
		const client2Subscription = client2.subscribe({ collection: "todos" })
		const seedTx = client2.transact()
		seedTx.set("todos", todo("todo-1", { text: "Original" }))
		await (
			await client2.commit(seedTx)
		).result
		await expectQuery(client1, { collection: "todos" }).toResolveTo([
			todo("todo-1", { text: "Original" }),
		])

		// Offline, client1 creates and deletes a draft in one transaction
		await (
			await client1.disconnect()
		).result
		const draftTx = client1.transact()
		draftTx.set("todos", todo("draft", { text: "Never kept" }))
		draftTx.remove("todos", "draft")
		await (
			await client1.commit(draftTx)
		).result

		// Reconnecting pulls a patch while the draft mutation is still pending
		const remoteTx = client2.transact()
		remoteTx.set("todos", todo("todo-1", { text: "Changed remotely" }))
		await (
			await client2.commit(remoteTx)
		).result
		await (
			await client1.connect()
		).result

		expect(client1.query({ collection: "todos" })).toEqual([
			todo("todo-1", { text: "Changed remotely" }),
		])
		client1Subscription.destroy()
		client2Subscription.destroy()
	})

	test("converges after both clients edit the same record concurrently", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		const client1Subscription = client1.subscribe({ collection: "todos" })
		const client2Subscription = client2.subscribe({ collection: "todos" })
		const seedTx = client1.transact()
		seedTx.set("todos", todo("shared", { text: "Original" }))
		await (
			await client1.commit(seedTx)
		).result
		await expectQuery(client2, { collection: "todos" }).toResolveTo([
			todo("shared", { text: "Original" }),
		])
		await gatekeeper.activateGates()

		// Both clients optimistically replace the same server record
		const client1Tx = client1.transact()
		client1Tx.set("todos", todo("shared", { text: "Client 1 edit" }))
		const client1Commit = await client1.commit(client1Tx)
		const client2Tx = client2.transact()
		client2Tx.set("todos", todo("shared", { text: "Client 2 edit" }))
		const client2Commit = await client2.commit(client2Tx)

		// Applying client2 last establishes the canonical record
		await client1Commit.continueToCompletion()
		await client2Commit.continueToCompletion()
		const client1Pull = await client1.pullFromRemote()
		await client1Pull.continueToCompletion()
		const client2Pull = await client2.pullFromRemote()
		await client2Pull.continueToCompletion()

		const canonicalTodos = [todo("shared", { text: "Client 2 edit" })]
		expect(client1.query({ collection: "todos" })).toEqual(canonicalTodos)
		expect(client2.query({ collection: "todos" })).toEqual(canonicalTodos)
		client1Subscription.destroy()
		client2Subscription.destroy()
	})

	test("converges after clients concurrently edit different records", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		const client1Subscription = client1.subscribe({ collection: "todos" })
		const client2Subscription = client2.subscribe({ collection: "todos" })
		await gatekeeper.activateGates()

		// Each client optimistically creates an independent record
		const client1Tx = client1.transact()
		client1Tx.set("todos", todo("client-1", { text: "From client 1" }))
		const client1Commit = await client1.commit(client1Tx)
		const client2Tx = client2.transact()
		client2Tx.set("todos", todo("client-2", { text: "From client 2" }))
		const client2Commit = await client2.commit(client2Tx)

		await client1Commit.continueToCompletion()
		await client2Commit.continueToCompletion()
		const client1Pull = await client1.pullFromRemote()
		await client1Pull.continueToCompletion()
		const client2Pull = await client2.pullFromRemote()
		await client2Pull.continueToCompletion()

		const canonicalTodos = [
			todo("client-1", { text: "From client 1" }),
			todo("client-2", { text: "From client 2" }),
		]
		expect(client1.query({ collection: "todos" })).toEqual(canonicalTodos)
		expect(client2.query({ collection: "todos" })).toEqual(canonicalTodos)
		client1Subscription.destroy()
		client2Subscription.destroy()
	})

	test("replays a later local mutation while pulling between queued pushes", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		const client1Subscription = client1.subscribe({ collection: "todos" })
		const client2Subscription = client2.subscribe({ collection: "todos" })
		const seedTx = client1.transact()
		seedTx.set("todos", todo("todo-1", { text: "First original" }))
		seedTx.set("todos", todo("todo-2", { text: "Second original" }))
		await (
			await client1.commit(seedTx)
		).result
		await expectQuery(client2, { collection: "todos" }).toResolveTo([
			todo("todo-1", { text: "First original" }),
			todo("todo-2", { text: "Second original" }),
		])
		await gatekeeper.activateGates()

		// Hold client2's first push while client1 advances the same server record
		const firstLocalTx = client2.transact()
		firstLocalTx.set("todos", todo("todo-1", { text: "First local edit" }))
		const firstLocalCommit = await client2.commit(firstLocalTx)
		const remoteTx = client1.transact()
		remoteTx.set("todos", todo("todo-1", { text: "Remote edit" }))
		const remoteCommit = await client1.commit(remoteTx)
		await remoteCommit.continueToCompletion()

		// A second local mutation queues behind the pull triggered by the remote edit
		const secondLocalTx = client2.transact()
		secondLocalTx.set("todos", todo("todo-2", { text: "Second local edit" }))
		const secondLocalReady = client2.commit(secondLocalTx)
		await firstLocalCommit.continueToCompletion()
		const secondLocalCommit = await secondLocalReady

		expect(client2.query({ collection: "todos" })).toEqual([
			todo("todo-1", { text: "First local edit" }),
			todo("todo-2", { text: "Second local edit" }),
		])

		await secondLocalCommit.continueToCompletion()
		const client1Pull = await client1.pullFromRemote()
		await client1Pull.continueToCompletion()
		const client2Pull = await client2.pullFromRemote()
		await client2Pull.continueToCompletion()

		const canonicalTodos = [
			todo("todo-1", { text: "First local edit" }),
			todo("todo-2", { text: "Second local edit" }),
		]
		expect(client1.query({ collection: "todos" })).toEqual(canonicalTodos)
		expect(client2.query({ collection: "todos" })).toEqual(canonicalTodos)
		client1Subscription.destroy()
		client2Subscription.destroy()
	})

	test("converges when a concurrent removal follows an update", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		const client1Subscription = client1.subscribe({ collection: "todos" })
		const client2Subscription = client2.subscribe({ collection: "todos" })
		const seedTx = client1.transact()
		seedTx.set("todos", todo("shared", { text: "Original" }))
		await (
			await client1.commit(seedTx)
		).result
		await expectQuery(client2, { collection: "todos" }).toResolveTo([
			todo("shared", { text: "Original" }),
		])
		await gatekeeper.activateGates()

		// Client1 updates while client2 removes the same record
		const updateTx = client1.transact()
		updateTx.set("todos", todo("shared", { text: "Updated" }))
		const update = await client1.commit(updateTx)
		const removeTx = client2.transact()
		removeTx.remove("todos", "shared")
		const remove = await client2.commit(removeTx)

		// Applying the removal last leaves every replica empty
		await update.continueToCompletion()
		await remove.continueToCompletion()
		const client1Pull = await client1.pullFromRemote()
		await client1Pull.continueToCompletion()
		const client2Pull = await client2.pullFromRemote()
		await client2Pull.continueToCompletion()

		expect(client1.query({ collection: "todos" })).toEqual([])
		expect(client2.query({ collection: "todos" })).toEqual([])
		client1Subscription.destroy()
		client2Subscription.destroy()
	})

	conflictTest(
		"replays a pending local edit on top of a newer remote patch",
		async ({ makeTodoGatekeeper, server }) => {
			const gate = Promise.withResolvers<void>()
			let delayedClientId = ""
			let delayedPushStarted = false
			const delayedServer: RemoteApi<TestsSchema> = {
				connect: (client) => server.connect(client),
				pull: (args) => server.pull(args),
				push: async (args) => {
					if (args.clientId === delayedClientId) {
						delayedPushStarted = true
						await gate.promise
					}
					return server.push(args)
				},
			}
			const gatekeeper = await makeTodoGatekeeper({ remote: delayedServer })
			const { client1, client2 } = gatekeeper
			delayedClientId = client2.clientId
			client1.subscribe({ collection: "todos" })
			client2.subscribe({ collection: "todos" })

			const seedTx = client1.transact()
			seedTx.set(
				"todos",
				todo("todo-1", { text: "Write the sync spec", priority: 2 }),
			)
			const seed = await client1.commit(seedTx)
			await seed.result
			await expectQuery(client2, { collection: "todos" }).toResolveTo([
				todo("todo-1", { text: "Write the sync spec", priority: 2 }),
			])
			await gatekeeper.activateGates()

			const localEditTx = client2.transact()
			localEditTx.set(
				"todos",
				todo("todo-1", { text: "Local edit on client 2", priority: 2 }),
			)
			const pendingCommit = await client2.commit(localEditTx)
			const pendingPush = pendingCommit.continueTo("server")
			await vi.waitFor(() => expect(delayedPushStarted).toBe(true))

			const remoteEditTx = client1.transact()
			remoteEditTx.set(
				"todos",
				todo("todo-1", {
					text: "Remote edit on client 1",
					done: true,
					priority: 5,
				}),
			)
			const remoteEdit = await client1.commit(remoteEditTx)
			await remoteEdit.continueToCompletion()
			await remoteEdit.result

			await expectQuery(client2, { collection: "todos" }).toResolveTo([
				todo("todo-1", { text: "Local edit on client 2", priority: 2 }),
			])

			gate.resolve()
			await pendingPush
			await pendingCommit.continueToCompletion()
			await pendingCommit.result
			await expectQuery(client1, { collection: "todos" }).toResolveTo([
				todo("todo-1", { text: "Local edit on client 2", priority: 2 }),
			])
		},
	)
})
