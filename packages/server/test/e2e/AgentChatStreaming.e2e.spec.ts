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

/** A streamed change, sent only after the server has persisted it. */
type ReplyDelta = { message: Omit<Message, "text">; delta: string }

/**
 * The application server: Tandem for durable state, plus a separate stream
 * that carries each reply delta to subscribed clients without a pull.
 */
class ChatServer {
	private readonly listeners = new Set<(event: ReplyDelta) => void>()

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

	subscribe(listener: (event: ReplyDelta) => void) {
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
			const { text: _text, ...message } = reply
			for (const listener of this.listeners) listener({ message, delta: token })
		}
	}
}

/** Fold a streamed delta into the visible message as a preview. */
function applyReplyDelta(
	client: TandemClient<ChatSchema, {}>,
	{ message, delta }: ReplyDelta,
) {
	const [visible] = client.query({
		collection: "messages",
		where: { id: message.id },
	})
	const seq = visible?.seq ?? 0
	// Older events are already visible. After a gap, wait for sync to catch up.
	if (message.seq !== seq + 1) return
	client.preview(
		"messages",
		{ ...message, text: (visible?.text ?? "") + delta },
		{ version: "seq" },
	)
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

test("a streamed reply only grows on the client and the server, then converges", async ({
	tandem,
}) => {
	const tokens = ["Tandem ", "keeps ", "one ", "writer ", "per ", "record."]
	const chat = new ChatServer(tandem, tokens)
	const client = new TandemClient<ChatSchema, {}>({
		schema,
		remote: chat.remote({ pullLatency: 3 }),
		logger: new Logger({ sinks: [] }),
		syncInterval: 0,
	})
	await client.ready
	const unsubscribeStream = chat.subscribe((event) =>
		applyReplyDelta(client, event),
	)

	const clientReplies: string[] = []
	const thread = client.subscribe(
		{ collection: "messages", where: { threadId: "thread-1" } },
		(messages) => {
			const reply = messages.find((message) => message.role === "assistant")
			if (reply && reply.text !== clientReplies.at(-1))
				clientReplies.push(reply.text)
		},
	)
	const serverReplies: string[] = []
	const serverThread = await tandem.subscribe(
		{ collection: "messages", where: { role: "assistant" } },
		([reply]) => {
			if (reply && reply.text !== serverReplies.at(-1))
				serverReplies.push(reply.text)
		},
	)

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
	await vi.waitFor(() => {
		expect(
			client.query({ collection: "messages", where: { role: "assistant" } }),
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

	// Neither copy ever moves backwards, and the client sees every delta. The
	// first delta can arrive before the pull that delivers the empty placeholder.
	const prefixes = tokens.map((_, index) => tokens.slice(0, index + 1).join(""))
	expect(clientReplies.filter((text) => text !== "")).toEqual(prefixes)
	expect(serverReplies).toEqual(["", ...prefixes])

	thread.destroy()
	serverThread.destroy()
	unsubscribeStream()
	await client.disconnect()
})
