import {
	collection,
	defineSchema,
	type LoggerApi,
	type RemoteApi,
	t,
	TandemClient,
	type TandemClientStorageApi,
} from "@tanishqkancharla/tandem-core"
import type { TandemServer } from "@tanishqkancharla/tandem-server"
import * as errore from "errore"
import { describe, expect } from "vitest"
import { buildGatekeeperHarness, test, type DemoRng } from "../fixtures.js"

type Message = { id: string; text: string; seq: number }
type MessagesSchema = { messages: Message }

const messagesSchema = defineSchema({
	messages: collection({ id: t.id(), text: t.string(), seq: t.number() }),
})

const message = (seq: number, text: string): Message => ({
	id: "reply",
	text,
	seq,
})

// Pokes stay manual so each test decides when a pull reads the server.
function manualPullRemote(
	server: TandemServer<MessagesSchema, {}>,
): RemoteApi<MessagesSchema> {
	return {
		connect: (client) => server.connect({ ...client, poke: async () => {} }),
		pull: (args) => server.pull(args),
		push: (args) => server.push(args),
	}
}

function createClient({
	remote,
	logger,
	rng,
	clientStorage,
}: {
	remote: RemoteApi<MessagesSchema>
	logger: LoggerApi
	rng: DemoRng
	clientStorage?: TandemClientStorageApi<MessagesSchema>
}) {
	return new TandemClient<MessagesSchema, {}>({
		schema: messagesSchema,
		remote,
		logger,
		rng: rng.create("client"),
		clientStorage,
		autoConnect: false,
		syncInterval: 0,
	})
}

const previewTest = test.extend<{
	messages: TandemServer<MessagesSchema, {}>
	client: TandemClient<MessagesSchema, {}>
	commitOnServer: (record: Message) => Promise<void>
}>({
	messages: async ({ makeRemote }, use) => {
		await use(makeRemote({ schema: messagesSchema, relations: {} }))
	},
	client: async ({ messages, logger, rng }, use) => {
		const client = createClient({
			remote: manualPullRemote(messages),
			logger,
			rng,
		})
		await client.ready
		await client.connect()
		client.subscribe({ collection: "messages" })
		await client.pullFromRemote()
		await use(client)
		await client.disconnect()
	},
	commitOnServer: async ({ messages }, use) => {
		await use(async (record) => {
			const tx = messages.transact()
			tx.set("messages", record)
			await messages.commit(tx)
		})
	},
})

describe("TandemClient previews", () => {
	previewTest(
		"a preview shows server output before sync and retires once sync catches up",
		async ({ client, messages, commitOnServer }) => {
			// The stream can outrun the pull that creates the record.
			client.preview("messages", message(2, "Hello"), { version: "seq" })
			await client.pullFromRemote()
			expect(client.query({ collection: "messages" })).toEqual([
				message(2, "Hello"),
			])

			await commitOnServer(message(1, "Hel"))
			await client.pullFromRemote()
			expect(client.query({ collection: "messages" })).toEqual([
				message(2, "Hello"),
			])
			// The preview was never pushed.
			expect(await messages.query({ collection: "messages" })).toEqual([
				message(1, "Hel"),
			])

			await commitOnServer(message(2, "Hello"))
			await client.pullFromRemote()
			await commitOnServer(message(3, "Hello world"))
			await client.pullFromRemote()
			expect(client.query({ collection: "messages" })).toEqual([
				message(3, "Hello world"),
			])

			// A late stream event cannot move the synced record backwards.
			client.preview("messages", message(2, "Hello"), { version: "seq" })
			expect(client.query({ collection: "messages" })).toEqual([
				message(3, "Hello world"),
			])
		},
	)

	previewTest("a preview ignores older stream events", async ({ client }) => {
		client.preview("messages", message(3, "Hello world"), { version: "seq" })
		client.preview("messages", message(2, "Hello"), { version: "seq" })

		expect(client.query({ collection: "messages" })).toEqual([
			message(3, "Hello world"),
		])
	})

	test("a pull that read older server state does not hide a newer preview", async ({
		makeRemote,
		logger,
		rng,
	}) => {
		const messages = makeRemote({ schema: messagesSchema, relations: {} })
		const commitOnServer = async (record: Message) => {
			const tx = messages.transact()
			tx.set("messages", record)
			await messages.commit(tx)
		}
		await using cleanup = new errore.AsyncDisposableStack()
		await using gatekeeper = buildGatekeeperHarness({
			server: manualPullRemote(messages),
			createClient: (remote) => {
				const client = createClient({ remote, logger, rng })
				cleanup.defer(() => client.disconnect())
				return client
			},
		})
		const { client1 } = gatekeeper
		await client1.ready
		await client1.connect()
		client1.subscribe({ collection: "messages" })
		await commitOnServer(message(1, "Hel"))
		await client1.pullFromRemote()

		await gatekeeper.activateGates()
		const stalePull = await client1.pullFromRemote()
		await stalePull.continueTo("server")
		// The server persists and streams seq 2 after it answered the pull.
		await commitOnServer(message(2, "Hello"))
		client1.preview("messages", message(2, "Hello"), { version: "seq" })
		await stalePull.continueToCompletion()

		expect(client1.query({ collection: "messages" })).toEqual([
			message(2, "Hello"),
		])
		await gatekeeper.deactivateGatesAndSettle()
	})

	test("a reset snapshot read before the record existed does not hide its preview", async ({
		makeRemote,
		logger,
		rng,
	}) => {
		const messages = makeRemote({ schema: messagesSchema, relations: {} })
		await using cleanup = new errore.AsyncDisposableStack()
		await using gatekeeper = buildGatekeeperHarness({
			server: manualPullRemote(messages),
			createClient: (remote) => {
				const client = createClient({ remote, logger, rng })
				cleanup.defer(() => client.disconnect())
				return client
			},
		})
		const { client1 } = gatekeeper
		await client1.ready
		await client1.connect()
		client1.subscribe({ collection: "messages" })
		await client1.pullFromRemote()

		// Losing a pull response makes the next pull a reset snapshot.
		await gatekeeper.activateGates()
		const lostPull = await client1.pullFromRemote()
		await lostPull.continueTo("server")
		await lostPull.fail(new Error("Lost response"))
		await expect(lostPull.result).rejects.toThrow()
		const resetPull = await client1.pullFromRemote()
		await resetPull.continueTo("server")
		const tx = messages.transact()
		tx.set("messages", message(1, "Hel"))
		await messages.commit(tx)
		client1.preview("messages", message(1, "Hel"), { version: "seq" })
		await resetPull.continueToCompletion()

		expect(client1.query({ collection: "messages" })).toEqual([
			message(1, "Hel"),
		])
		await gatekeeper.deactivateGatesAndSettle()
	})

	previewTest(
		"a late stream event cannot bring back a record the server removed",
		async ({ client, messages, commitOnServer }) => {
			await commitOnServer(message(1, "Hel"))
			await client.pullFromRemote()
			const tx = messages.transact()
			tx.remove("messages", "reply")
			await messages.commit(tx)
			await client.pullFromRemote()

			client.preview("messages", message(1, "Hel"), { version: "seq" })
			expect(client.query({ collection: "messages" })).toEqual([])

			// Once a pull delivers the record again, previews apply again.
			await commitOnServer(message(2, "Hi"))
			await client.pullFromRemote()
			client.preview("messages", message(3, "Hi there"), { version: "seq" })
			expect(client.query({ collection: "messages" })).toEqual([
				message(3, "Hi there"),
			])
		},
	)

	previewTest(
		"a pull that removes the record drops its preview",
		async ({ client, messages, commitOnServer }) => {
			await commitOnServer(message(1, "Hel"))
			await client.pullFromRemote()
			client.preview("messages", message(2, "Hello"), { version: "seq" })

			const tx = messages.transact()
			tx.remove("messages", "reply")
			await messages.commit(tx)
			await client.pullFromRemote()

			expect(client.query({ collection: "messages" })).toEqual([])
		},
	)

	previewTest(
		"clearPreview shows the synced record again",
		async ({ client, commitOnServer }) => {
			await commitOnServer(message(1, "Hel"))
			await client.pullFromRemote()
			client.preview("messages", message(2, "Hello"), { version: "seq" })

			client.clearPreview("messages", "reply")

			expect(client.query({ collection: "messages" })).toEqual([
				message(1, "Hel"),
			])
		},
	)

	previewTest(
		"a local write replaces the preview of the record it writes",
		async ({ client, messages, commitOnServer }) => {
			await commitOnServer(message(1, "Hel"))
			await client.pullFromRemote()
			client.preview("messages", message(5, "Hello"), { version: "seq" })

			const tx = client.transact()
			tx.set("messages", message(1, "Edited"))
			await client.commit(tx)
			await client.pullFromRemote()

			expect(client.query({ collection: "messages" })).toEqual([
				message(1, "Edited"),
			])
			expect(await messages.query({ collection: "messages" })).toEqual([
				message(1, "Edited"),
			])
		},
	)

	test("a restarted client shows the synced record, not an unconfirmed preview", async ({
		makeRemote,
		makeStorage,
		logger,
		rng,
	}) => {
		const messages = makeRemote({ schema: messagesSchema, relations: {} })
		const tx = messages.transact()
		tx.set("messages", message(1, "Hel"))
		await messages.commit(tx)
		const storage = makeStorage<MessagesSchema>({ schema: messagesSchema })
		const remote = manualPullRemote(messages)

		const first = createClient({ remote, logger, rng, clientStorage: storage })
		await first.ready
		await first.connect()
		first.subscribe({ collection: "messages" })
		await first.pullFromRemote()
		// The server never persists seq 2, for example because inference crashed.
		first.preview("messages", message(2, "Hello"), { version: "seq" })
		await first.flushClientStorage()
		await first.disconnect()

		const restarted = createClient({
			remote,
			logger,
			rng,
			clientStorage: storage,
		})
		await restarted.ready

		// Even before reconnecting, the restarted client cannot track the preview.
		expect(restarted.query({ collection: "messages" })).toEqual([
			message(1, "Hel"),
		])
	})

	test("preview versions must be numeric record fields", () => {
		const client = new TandemClient<MessagesSchema, {}>({
			schema: messagesSchema,
		})
		expect(() =>
			// @ts-expect-error Previews compare a numeric field such as a sequence number.
			client.preview("messages", message(1, "Hel"), { version: "text" }),
		).toThrow("Previews require a remote")
	})
})
