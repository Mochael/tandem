import {
	schema,
	type TodoRemoteRequest,
	type TodoSchema,
} from "@tandem/example-todo-shared"
import { matchBy, tag, type ClientId } from "@tanishqkancharla/tandem-core"
import {
	createTandemEventStreamResponse,
	TandemServer,
	TandemServerJsonFileStorage,
} from "@tanishqkancharla/tandem-server"
import * as errore from "errore"
import { Hono } from "hono"

class TodoHonoError extends errore.createTaggedError({
	name: "TodoHonoError",
	message: "Todo HTTP server failed to $operation",
}) {}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value)
}

function isTodoRemoteRequest(value: unknown): value is TodoRemoteRequest {
	return (
		isRecord(value) &&
		(value.action === "pull" || value.action === "push") &&
		isRecord(value.args)
	)
}

function validateRequest(value: unknown) {
	if (isTodoRemoteRequest(value)) return value
	return new TodoHonoError({ operation: "validate request" })
}

type TodoServer = TandemServer<TodoSchema, {}>

function executeRequest({
	server,
	request,
}: {
	server: TodoServer
	request: TodoRemoteRequest
}) {
	return matchBy(request, "action", {
		push: ({ args }) =>
			server
				.push(args)
				.catch(
					(cause) => new TodoHonoError({ operation: "push mutation", cause }),
				),
		pull: ({ args }) =>
			server
				.pull(args)
				.catch(
					(cause) => new TodoHonoError({ operation: "pull changes", cause }),
				),
	})
}

async function seedIfEmpty(server: TodoServer) {
	const todos = await server
		.query({ collection: "todos" })
		.catch((cause) => new TodoHonoError({ operation: "read seed data", cause }))
	if (todos instanceof Error) return todos
	if (todos.length > 0) return

	const tx = server.transact()
	tx.set("todos", {
		id: "welcome",
		text: "Build something with Tandem",
		complete: false,
		createdAt: Date.now(),
	})
	tx.set("todos", {
		id: "maui",
		text: "Style it with Maui color tokens",
		complete: true,
		createdAt: Date.now() - 1,
	})
	return server
		.commit(tx)
		.catch(
			(cause) => new TodoHonoError({ operation: "write seed data", cause }),
		)
}

async function createTodoServer({ filePath }: { filePath: string }) {
	const server = new TandemServer({
		schema,
		relations: {},
		storage: new TandemServerJsonFileStorage<TodoSchema>({ filePath }),
	})
	const seeded = await seedIfEmpty(server)
	if (!(seeded instanceof Error)) return server

	const closed = await server
		.close()
		.catch((cause) => new TodoHonoError({ operation: "close storage", cause }))
	if (closed instanceof Error) console.error(closed)
	return seeded
}

export async function createTodoApp({ filePath }: { filePath: string }) {
	const server = await createTodoServer({ filePath })
	if (server instanceof Error) return server

	// Long-lived streams would otherwise keep the HTTP server from closing.
	const eventStreams = new AbortController()
	const app = new Hono()
	app.get("/health", (context) => context.json({ status: "ok" }))
	app.get("/api/tandem/events", async (context) => {
		const clientId = context.req.query("clientId")
		if (!clientId) {
			return context.json(
				{ error: "invalid-request", message: "Missing clientId" },
				400,
			)
		}
		return await createTandemEventStreamResponse({
			remote: server,
			clientId: tag<ClientId>(clientId),
			signal: AbortSignal.any([context.req.raw.signal, eventStreams.signal]),
		})
	})
	app.post("/api/tandem", async (context) => {
		const json = await context.req
			.json<unknown>()
			.catch(
				(cause) => new TodoHonoError({ operation: "parse request", cause }),
			)
		if (json instanceof Error) {
			return context.json(
				{ error: "invalid-request", message: "Invalid Tandem request" },
				400,
			)
		}

		const request = validateRequest(json)
		if (request instanceof Error) {
			return context.json(
				{ error: "invalid-request", message: "Invalid Tandem request" },
				400,
			)
		}

		const result = await executeRequest({ server, request })
		if (result instanceof Error) {
			console.error(result)
			return context.json({ error: "Tandem request failed" }, 500)
		}

		return context.json(result)
	})

	return {
		app,
		endEventStreams: () => eventStreams.abort(),
		close: () => {
			eventStreams.abort()
			return server.close()
		},
	}
}
