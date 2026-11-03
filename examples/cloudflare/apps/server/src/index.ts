import {
	matchBy,
	MutationApi,
	tag,
	type ClientId,
} from "@tanishqkancharla/tandem-core"
import {
	TandemServer,
	createTandemEventStreamResponse,
} from "@tanishqkancharla/tandem-server"
import { TandemServerSqlStorage } from "@tanishqkancharla/tandem-server/sql"
import * as errore from "errore"
import { schema, type AppSchema, type SyncRequest } from "./schema.js"

type Env = { DB: D1Database; SYNC: DurableObjectNamespace }

class SyncHttpError extends errore.createTaggedError({
	name: "SyncHttpError",
	message: "Sync HTTP failed to $operation",
}) {}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value)
}

function decodeRequest(value: unknown) {
	if (!isRecord(value) || !isRecord(value.args))
		return new SyncHttpError({ operation: "read the request envelope" })
	const args = value.args
	if (typeof args.clientId !== "string")
		return new SyncHttpError({ operation: "read clientId" })
	if (value.action === "pull" && Array.isArray(args.scanWindow))
		return value as SyncRequest
	if (value.action !== "push" || !Array.isArray(args.mutations))
		return new SyncHttpError({ operation: "read a push or pull" })
	return errore.try({
		try: () => ({
			action: "push" as const,
			args: {
				clientId: tag<ClientId>(args.clientId as string),
				mutations: (args.mutations as unknown[]).map((mutation) =>
					MutationApi.codec(schema).decode(mutation),
				),
			},
		}),
		catch: (cause) =>
			new SyncHttpError({ operation: "decode mutations", cause }),
	})
}

export class SyncObject {
	private readonly server: TandemServer<AppSchema, {}>

	constructor(_ctx: DurableObjectState, env: Env) {
		this.server = new TandemServer({
			schema,
			relations: {},
			storage: new TandemServerSqlStorage<AppSchema>({
				dialect: "sqlite",
				tables: {
					tasks: {
						table: "tasks",
						idColumns: ["task_id"],
						encode: ({ text, done }) => ({
							title: text,
							is_done: Number(done),
						}),
						decode: (row) => ({
							id: row.task_id as string,
							text: row.title as string,
							done: row.is_done === 1,
						}),
					},
				},
				query: async ({ sql, parameters }) =>
					(
						await env.DB.prepare(sql)
							.bind(...parameters)
							.all()
					).results,
				commit: async (statements) => {
					await env.DB.batch(
						statements.map(({ sql, parameters }) =>
							env.DB.prepare(sql).bind(...parameters),
						),
					)
				},
			}),
		})
	}

	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url)
		if (request.method === "GET" && url.pathname === "/api/tandem/events") {
			const clientId = url.searchParams.get("clientId")
			if (!clientId) return new Response("Missing clientId", { status: 400 })
			return createTandemEventStreamResponse({
				remote: this.server,
				clientId: tag<ClientId>(clientId),
				schema,
				signal: request.signal,
			})
		}
		if (request.method !== "POST" || url.pathname !== "/api/tandem")
			return new Response("Not found", { status: 404 })
		const body: unknown = await request
			.json()
			.catch((cause) => new SyncHttpError({ operation: "parse JSON", cause }))
		if (body instanceof Error)
			return new Response(body.message, { status: 400 })
		const decoded = decodeRequest(body)
		if (decoded instanceof Error)
			return new Response(decoded.message, { status: 400 })
		const result = await matchBy(decoded, "action", {
			push: ({ args }) => this.server.push(args),
			pull: ({ args }) => this.server.pull(args),
		})
		return Response.json(result)
	}
}

export default {
	fetch(request: Request, env: Env): Promise<Response> {
		// Put the application's authentication and authorization here, including
		// the SSE route. This example routes one application to one object.
		return env.SYNC.getByName("app").fetch(request)
	},
}
