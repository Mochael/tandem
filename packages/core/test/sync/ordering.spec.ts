import { describe, expect } from "vitest"
import { test, todo } from "../fixtures.js"

describe("Tandem client sync ordering", () => {
	test("syncs a write committed synchronously by a subscription callback", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		client2.subscribe({ collection: "todos" })
		let nestedCommit: ReturnType<typeof client1.commit> | undefined
		let committed = false
		const first = todo("first", { text: "Outer write" })
		const second = todo("second", { text: "Subscriber write" })
		const subscription = client1.subscribe(
			{ collection: "todos" },
			(records) => {
				if (committed || !records.some((record) => record.id === first.id))
					return
				committed = true
				const nestedTx = client1.transact()
				nestedTx.set("todos", second)
				nestedCommit = client1.commit(nestedTx)
			},
		)
		const tx = client1.transact()
		tx.set("todos", first)
		await (
			await client1.commit(tx)
		).result
		await (
			await nestedCommit
		)?.result
		await (
			await client1.pullFromRemote()
		).result
		await (
			await client2.pullFromRemote()
		).result

		expect(committed).toBe(true)
		expect(client1.query({ collection: "todos" })).toEqual([first, second])
		expect(client2.query({ collection: "todos" })).toEqual([first, second])
		subscription.destroy()
	})

	test("shows a newer edit while the previous push is in flight", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		client2.subscribe({ collection: "todos" })
		const firstTodo = todo("queued", { text: "First title" })
		const firstTx = client1.transact()
		firstTx.set("todos", firstTodo)

		await gatekeeper.activateGates()
		const first = await client1.commit(firstTx)
		const secondTodo = todo("queued", { text: "Final title" })
		const secondTx = client1.transact()
		secondTx.set("todos", secondTodo)
		void client1.commit(secondTx)

		first.assertSentBy("client1").assertWaitingFor("server")
		expect(client1.query({ collection: "todos" })).toEqual([secondTodo])

		// The in-flight request is a snapshot, not the list extended by the new edit.
		await first.continueTo("server")
		const pull = await client2.pullFromRemote()
		await pull.continueToCompletion()
		expect(client2.query({ collection: "todos" })).toEqual([firstTodo])
	})

	test("makes a server-accepted edit visible before acknowledging it", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		const subscription = client2.subscribe({ collection: "todos" })
		const acceptedTodo = todo("accepted", { text: "Accepted title" })
		const tx = client1.transact()
		tx.set("todos", acceptedTodo)

		await gatekeeper.activateGates()
		const commit = await client1.commit(tx)
		await commit.continueTo("server")
		const pull = await client2.pullFromRemote()
		await pull.continueToCompletion()

		commit.assertSentBy("server").assertWaitingFor("client1")
		pull.assertCompleted()
		expect(client2.query({ collection: "todos" })).toEqual([acceptedTodo])
		subscription.destroy()
	})

	test("releases a queued edit after acknowledging the previous push", async ({
		gatekeeper,
	}) => {
		const { client1 } = gatekeeper
		const firstTodo = todo("queued", { text: "First title" })
		const firstTx = client1.transact()
		firstTx.set("todos", firstTodo)

		await gatekeeper.activateGates()
		const first = await client1.commit(firstTx)
		const secondTodo = todo("queued", { text: "Final title" })
		const secondTx = client1.transact()
		secondTx.set("todos", secondTodo)
		const secondReady = client1.commit(secondTx)

		await first.continueTo("server")
		await first.continueTo("client1")
		const second = await secondReady
		second.assertSentBy("client1").assertWaitingFor("server")
		await second.continueToCompletion()

		first.assertCompleted()
		expect(await first.result).toBeUndefined()
		second.assertCompleted()
		expect(client1.query({ collection: "todos" })).toEqual([secondTodo])
	})

	test("syncs a queued edit after the previous push is acknowledged", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		const subscription = client2.subscribe({ collection: "todos" })
		const firstTx = client1.transact()
		firstTx.set("todos", todo("queued", { text: "First title" }))
		await gatekeeper.activateGates()
		const first = await client1.commit(firstTx)
		const secondTodo = todo("queued", { text: "Final title" })
		const secondTx = client1.transact()
		secondTx.set("todos", secondTodo)
		const secondReady = client1.commit(secondTx)

		await first.continueToCompletion()
		const second = await secondReady
		await second.continueToCompletion()
		const pull = await client2.pullFromRemote()
		await pull.continueToCompletion()

		first.assertCompleted()
		expect(await first.result).toBeUndefined()
		second.assertCompleted()
		pull.assertCompleted()
		expect(client2.query({ collection: "todos" })).toEqual([secondTodo])
		subscription.destroy()
	})

	test("rejects a commit when its acknowledgement is lost", async ({
		gatekeeper,
	}) => {
		const { client1 } = gatekeeper
		const tx = client1.transact()
		tx.set("todos", todo("lost-ack", { text: "Accepted title" }))
		const failure = new Error("Connection closed before acknowledgement")

		await gatekeeper.activateGates()
		const commit = await client1.commit(tx)
		await commit.continueTo("server")

		await commit.fail(failure)

		commit.assertCompleted()
		await expect(commit.result).rejects.toBe(failure)
	})

	test("keeps a server-accepted edit after its acknowledgement is lost", async ({
		gatekeeper,
	}) => {
		const { client1, client2 } = gatekeeper
		const recoveringSubscription = client1.subscribe({ collection: "todos" })
		const subscription = client2.subscribe({ collection: "todos" })
		await (
			await client1.pullFromRemote()
		).result
		await (
			await client2.pullFromRemote()
		).result
		const acceptedTodo = todo("accepted-without-ack", {
			text: "Stored despite the lost response",
		})
		const tx = client1.transact()
		tx.set("todos", acceptedTodo)
		const failure = new Error("Connection closed before acknowledgement")

		await gatekeeper.activateGates()
		const commit = await client1.commit(tx)
		const commitSettled = Promise.allSettled([commit.result])
		await commit.continueTo("server")
		await commit.fail(failure)
		expect(client1.query({ collection: "todos" })).toEqual([acceptedTodo])
		const pull = await client2.pullFromRemote()
		await pull.continueToCompletion()

		pull.assertCompleted()
		expect(client2.query({ collection: "todos" })).toEqual([acceptedTodo])

		await commitSettled
		const recoveryPull = await client1.pullFromRemote()
		await recoveryPull.continueToCompletion()

		recoveryPull.assertCompleted()
		expect(client1.query({ collection: "todos" })).toEqual([acceptedTodo])
		recoveringSubscription.destroy()
		subscription.destroy()
	})
})
