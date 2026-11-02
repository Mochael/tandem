import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import {
	collection,
	defineSchema,
	Logger,
	type RemoteApi,
	TandemClient,
} from "@tanishqkancharla/tandem-core"
import {
	TandemServer,
	TandemServerJsonFileStorage,
} from "@tanishqkancharla/tandem-server"
import * as errore from "errore"
import { expect, test as baseTest, vi } from "vitest"

// The reference shape for agent chat on Tandem, from docs/agent-chat.md.

type Message = {
	id: string
	threadId: string
	role: "user" | "assistant"
	text: string
	/** Bumped by the server on every persisted change to an assistant message. */
	seq: number
	status: "streaming" | "complete"
}

type ChatSchema = { messages: Message }

const schema = defineSchema({
	messages: collection<Message>({
		fields: ["id", "threadId", "role", "text", "seq", "status"],
	}),
})

/**
 * The application server: Tandem for durable state, plus a separate stream
 * that sends each saved reply to every viewer of the thread without a pull.
 */
class ChatServer {
	private readonly listeners = new Set<(reply: Message) => void>()

	constructor(
		readonly tandem: TandemServer<ChatSchema, {}>,
		private readonly tokens: string[],
	) {}

	/** The transport a browser client would reach over HTTP. */
	remote({ pullLatency }: { pullLatency: number }): RemoteApi<ChatSchema> {
		return {
			connect: (client) => this.tandem.connect(client),
			push: async (args) => {
				// Clients may only send user messages. Assistant output has one writer.
				const userMessages: Message[] = []
				for (const { ops } of args.mutations) {
					for (const op of ops) {
						if (op.type !== "set" || op.value.role !== "user") {
							return {
								error: "invalid-request",
								message: "Only the server writes assistant messages",
							}
						}
						userMessages.push(op.value)
					}
				}
				const response = await this.tandem.push(args)
				if ("error" in response) return response
				for (const message of userMessages) void this.reply(message)
				return response
			},
			pull: async (args) => {
				const response = await this.tandem.pull(args)
				// Deliver the snapshot late, as a slow network would.
				await new Promise((resolve) => setTimeout(resolve, pullLatency))
				return response
			},
		}
	}

	subscribe(listener: (reply: Message) => void) {
		this.listeners.add(listener)
		return () => this.listeners.delete(listener)
	}

	private async reply(userMessage: Message) {
		const id = `${userMessage.id}:reply`
		const claim = this.tandem.transact()
		// Retried pushes must not start a second reply.
		if (await claim.get("messages", id)) return claim.cancel()
		let reply: Message = {
			id,
			threadId: userMessage.threadId,
			role: "assistant",
			text: "",
			seq: 0,
			status: "streaming",
		}
		claim.set("messages", reply)
		await this.tandem.commit(claim)

		for (const [index, token] of this.tokens.entries()) {
			await new Promise((resolve) => setTimeout(resolve, 1))
			reply = {
				...reply,
				text: reply.text + token,
				seq: reply.seq + 1,
				status: index === this.tokens.length - 1 ? "complete" : "streaming",
			}
			// Persist first, so every streamed seq is one a pull can eventually return.
			const tx = this.tandem.transact()
			tx.set("messages", reply)
			await this.tandem.commit(tx)
			for (const listener of this.listeners) listener(reply)
		}
	}
}

const test = baseTest.extend<{ tandem: TandemServer<ChatSchema, {}> }>({
	tandem: async ({}, use) => {
		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "tandem-agent-chat-e2e-"),
		)
		const tandem = new TandemServer({
			schema,
			relations: {},
			storage: new TandemServerJsonFileStorage<ChatSchema>({
				filePath: path.join(directory, "tandem.json"),
			}),
		})
		await use(tandem)
		await tandem.close()
		await fs.rm(directory, { recursive: true, force: true })
	},
})

test("a streamed reply only grows for every viewer and the server, then converges", async ({
	tandem,
}) => {
	const tokens = ["Tandem ", "keeps ", "one ", "writer ", "per ", "record."]
	const chat = new ChatServer(tandem, tokens)
	await using cleanup = new errore.AsyncDisposableStack()

	// The sender and a friend watching the same thread see the same stream.
	const viewers = ["client", "friend"].map(() => {
		const viewer = new TandemClient<ChatSchema, {}>({
			schema,
			remote: chat.remote({ pullLatency: 3 }),
			logger: new Logger({ sinks: [] }),
			syncInterval: 0,
		})
		const replies: string[] = []
		const thread = viewer.subscribe(
			{ collection: "messages", where: { threadId: "thread-1" } },
			(messages) => {
				const reply = messages.find((message) => message.role === "assistant")
				if (reply && reply.text !== replies.at(-1)) replies.push(reply.text)
			},
		)
		const unsubscribeStream = chat.subscribe((reply) =>
			viewer.preview("messages", reply, { version: "seq" }),
		)
		cleanup.defer(async () => {
			unsubscribeStream()
			thread.destroy()
			await viewer.disconnect()
		})
		return { viewer, replies }
	})
	const [{ viewer: client }] = viewers
	await Promise.all(viewers.map(({ viewer }) => viewer.ready))

	const serverReplies: string[] = []
	const serverThread = await tandem.subscribe(
		{ collection: "messages", where: { role: "assistant" } },
		([reply]) => {
			if (reply && reply.text !== serverReplies.at(-1))
				serverReplies.push(reply.text)
		},
	)
	cleanup.defer(() => serverThread.destroy())

	const tx = client.transact()
	tx.set("messages", {
		id: "message-1",
		threadId: "thread-1",
		role: "user",
		text: "Why does streaming need a preview?",
		seq: 0,
		status: "complete",
	})
	await client.commit(tx)

	const finalReply = tokens.join("")
	for (const { viewer } of viewers) {
		await vi.waitFor(() => {
			expect(
				viewer.query({ collection: "messages", where: { role: "assistant" } }),
			).toEqual([
				{
					id: "message-1:reply",
					threadId: "thread-1",
					role: "assistant",
					text: finalReply,
					seq: tokens.length,
					status: "complete",
				},
			])
		})
	}

	// No copy ever moves backwards, and both viewers see every chunk. The first
	// chunk can arrive before the pull that delivers the empty placeholder.
	const prefixes = tokens.map((_, index) => tokens.slice(0, index + 1).join(""))
	for (const { replies } of viewers) {
		expect(replies.filter((text) => text !== "")).toEqual(prefixes)
	}
	expect(serverReplies).toEqual(["", ...prefixes])
})
