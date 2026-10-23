import { collection, defineSchema, tag } from "@tanishqkancharla/tandem-core"
import type { ClientId, MutationId } from "@tanishqkancharla/tandem-core"
import { expect, test } from "vitest"
import { createTandemEventStreamResponse, TandemServer } from "../src/index.js"
import { TestTandemServerStorage } from "./TandemServerStorage.fixture.js"

type User = { id: string; name: string }
type TestSchema = { users: User }

const schema = defineSchema({
	users: collection<User>({ fields: ["id", "name"] }),
})

function createServer() {
	return new TandemServer({
		schema,
		relations: {},
		storage: new TestTandemServerStorage<TestSchema>(),
	})
}

function readEvents(response: Response) {
	const reader = response.body!.pipeThrough(new TextDecoderStream()).getReader()
	let buffered = ""
	return {
		reader,
		async next(): Promise<string | undefined> {
			for (;;) {
				const end = buffered.indexOf("\n\n")
				if (end !== -1) {
					const frame = buffered.slice(0, end)
					buffered = buffered.slice(end + 2)
					return frame
				}
				const { done, value } = await reader.read()
				if (done) return undefined
				buffered += value
			}
		},
	}
}

test("streams a ready event and then pokes for server and client writes", async () => {
	const server = createServer()
	const response = await createTandemEventStreamResponse({
		remote: server,
		clientId: tag<ClientId>("observer"),
	})
	expect(response.status).toBe(200)
	expect(response.headers.get("content-type")).toBe(
		"text/event-stream; charset=utf-8",
	)
	expect(response.headers.get("cache-control")).toBe("no-cache, no-transform")
	const events = readEvents(response)

	// Ready arrives after registration, so a pull started from it sees later writes.
	expect(await events.next()).toBe("event: ready\ndata:")

	// A server-side commit pokes the stream.
	const tx = server.transact()
	tx.set("users", { id: "user-1", name: "Ada" })
	await server.commit(tx)
	expect(await events.next()).toBe("event: poke\ndata:")

	// Another client's push pokes the stream.
	await server.push({
		clientId: tag<ClientId>("writer"),
		mutations: [
			{
				id: tag<MutationId>(1),
				ops: [
					{
						type: "set",
						collection: "users",
						value: { id: "user-2", name: "Grace" },
					},
				],
			},
		],
	})
	expect(await events.next()).toBe("event: poke\ndata:")

	await events.reader.cancel()
	await server.close()
})

test("sends keep-alive comments while idle", async () => {
	const server = createServer()
	const response = await createTandemEventStreamResponse({
		remote: server,
		clientId: tag<ClientId>("idle"),
		heartbeatInterval: 5,
	})
	const events = readEvents(response)
	expect(await events.next()).toBe("event: ready\ndata:")
	expect(await events.next()).toBe(": keep-alive")

	await events.reader.cancel()
	await server.close()
})

test("closing the request or the body unregisters the stream", async () => {
	const server = createServer()
	let registrations = 0
	const remote = {
		connect: async (api: Parameters<typeof server.connect>[0]) => {
			const disconnect = await server.connect(api)
			registrations += 1
			return () => {
				registrations -= 1
				return disconnect()
			}
		},
	}

	// Aborting the request ends the body and releases the registration.
	const request = new AbortController()
	const aborted = readEvents(
		await createTandemEventStreamResponse({
			remote,
			clientId: tag<ClientId>("aborted"),
			signal: request.signal,
		}),
	)
	expect(await aborted.next()).toBe("event: ready\ndata:")
	request.abort()
	expect(await aborted.next()).toBeUndefined()
	expect(registrations).toBe(0)

	// A client that drops the connection cancels the body, which also releases it.
	const cancelled = readEvents(
		await createTandemEventStreamResponse({
			remote,
			clientId: tag<ClientId>("cancelled"),
		}),
	)
	expect(await cancelled.next()).toBe("event: ready\ndata:")
	await cancelled.reader.cancel()
	expect(registrations).toBe(0)

	// A request aborted before registration finishes never stays registered.
	const early = new AbortController()
	early.abort()
	const response = await createTandemEventStreamResponse({
		remote,
		clientId: tag<ClientId>("early"),
		signal: early.signal,
	})
	expect(await readEvents(response).next()).toBeUndefined()
	expect(registrations).toBe(0)

	await server.close()
})

test("responds unavailable when the remote cannot register the stream", async () => {
	const response = await createTandemEventStreamResponse({
		remote: {
			connect: () =>
				Promise.resolve({ error: "unavailable", message: "Remote is down" }),
		},
		clientId: tag<ClientId>("rejected"),
	})
	expect(response.status).toBe(503)
	expect(await response.json()).toEqual({
		error: "unavailable",
		message: "Remote is down",
	})
})
