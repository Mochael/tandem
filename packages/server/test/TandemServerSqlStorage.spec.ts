import { PGlite } from "@electric-sql/pglite"
import {
	collection,
	defineSchema,
	tag,
	TandemClient,
	type ClientId,
	type MutationId,
} from "@tanishqkancharla/tandem-core"
import * as errore from "errore"
import { Miniflare } from "miniflare"
import mysql from "mysql2/promise"
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest"
import { TandemServer } from "../src/index.js"
import {
	TandemServerSqlStorage,
	type TandemServerSqlStorageArgs,
	type TandemSqlDialect,
	type TandemSqlRow,
} from "../src/storage/TandemServerSqlStorage.js"

type Schema = {
	tasks: { id: string; text: string; done: boolean }
	messages: { id: readonly [string, number]; body: string }
}

const schema = defineSchema({
	tasks: collection<Schema["tasks"]>({ fields: ["id", "text", "done"] }),
	messages: collection<Schema["messages"]>({ fields: ["id", "body"] }),
})

const tables: TandemServerSqlStorageArgs<Schema>["tables"] = {
	tasks: {
		table: "app_tasks",
		idColumns: ["task_id"],
		encode: ({ text, done }) => ({ task_text: text, is_done: Number(done) }),
		decode: (row) => ({
			id: row.task_id as string,
			text: row.task_text as string,
			done: row.is_done === 1,
		}),
	},
	messages: {
		table: "app_messages",
		idColumns: ["conversation_id", "sequence"],
		encode: ({ body }) => ({ body }),
		decode: (row) => ({
			id: [row.conversation_id as string, row.sequence as number],
			body: row.body as string,
		}),
	},
}

type SqlDatabase = Pick<
	TandemServerSqlStorageArgs<Schema>,
	"query" | "commit"
> & {
	close: () => Promise<void>
}

async function database(dialect: TandemSqlDialect): Promise<SqlDatabase> {
	if (dialect === "sqlite") {
		const runtime = new Miniflare({
			modules: true,
			script: "export default {fetch() {return new Response('ok')}}",
			compatibilityDate: "2026-07-01",
			d1Databases: { DB: "sql-storage-test" },
		})
		const db = await runtime.getD1Database("DB")
		return {
			query: async ({ sql, parameters }) =>
				(
					await db
						.prepare(sql)
						.bind(...parameters)
						.all()
				).results,
			commit: async (statements) => {
				await db.batch(
					statements.map(({ sql, parameters }) =>
						db.prepare(sql).bind(...parameters),
					),
				)
			},
			close: () => runtime.dispose(),
		}
	}
	if (dialect === "postgres") {
		const db = new PGlite()
		return {
			query: async ({ sql, parameters }) =>
				(await db.query<TandemSqlRow>(sql, [...parameters])).rows,
			commit: async (statements) => {
				await db.transaction(async (tx) => {
					for (const { sql, parameters } of statements)
						await tx.query(sql, [...parameters])
				})
			},
			close: () => db.close(),
		}
	}
	const db = await mysql.createConnection(process.env.TANDEM_MYSQL_URL!)
	return {
		query: async ({ sql, parameters }) => {
			const [rows] = await db.query(sql, [...parameters])
			return rows as TandemSqlRow[]
		},
		commit: async (statements) => {
			await db.beginTransaction()
			await using rollback = new errore.AsyncDisposableStack()
			rollback.defer(() => db.rollback())
			for (const { sql, parameters } of statements)
				await db.query(sql, [...parameters])
			await db.commit()
		},
		close: () => db.end(),
	}
}

for (const dialect of ["sqlite", "postgres", "mysql"] as const) {
	describe.skipIf(dialect === "mysql" && !process.env.TANDEM_MYSQL_URL)(
		`${dialect} existing tables`,
		() => {
			let db: SqlDatabase
			let storage: TandemServerSqlStorage<Schema>
			let server: TandemServer<Schema, {}>

			beforeAll(async () => {
				db = await database(dialect)
			}, 30_000)
			afterAll(async () => {
				await db?.close()
			})
			beforeEach(async () => {
				for (const sql of [
					"drop table if exists app_tasks",
					"drop table if exists app_messages",
					"create table app_tasks (task_id varchar(255) primary key, task_text varchar(255) not null, is_done integer not null, audit_note varchar(255) default 'keep me', check (task_text <> 'forbidden'))",
					"create table app_messages (conversation_id varchar(255), sequence integer, body text not null, primary key (conversation_id, sequence))",
				])
					await db.commit([{ sql, parameters: [] }])
				storage = new TandemServerSqlStorage({ dialect, tables, ...db })
				server = new TandemServer({ schema, relations: {}, storage })
			})

			test("reads existing rows and preserves application columns on update", async () => {
				await db.commit([
					{
						sql: "alter table app_tasks add column owner_id varchar(255) not null",
						parameters: [],
					},
				])
				await db.commit([
					{
						sql: "insert into app_tasks (task_id, task_text, is_done, audit_note, owner_id) values ('a', 'Existing', 0, 'Owned by app', 'owner-1')",
						parameters: [],
					},
				])
				expect(await server.query({ collection: "tasks" })).toEqual([
					{ id: "a", text: "Existing", done: false },
				])
				const tx = server.transact()
				await tx.update("tasks", "a", { text: "Edited", done: true })
				await server.commit(tx)
				expect(
					await db.query({ sql: "select * from app_tasks", parameters: [] }),
				).toEqual([
					{
						task_id: "a",
						task_text: "Edited",
						is_done: 1,
						audit_note: "Owned by app",
						owner_id: "owner-1",
					},
				])
			})

			test("scans compound IDs in numeric order and deletes only the selected row", async () => {
				const tx = server.transact()
				for (const sequence of [2, 10, 1])
					tx.set("messages", { id: ["room", sequence], body: `${sequence}` })
				tx.set("messages", { id: ["other", 1], body: "other room" })
				await server.commit(tx)
				const read = server.transact()
				expect(
					await read.scan("messages", {
						prefix: ["room"],
						reverse: true,
						limit: 2,
					}),
				).toEqual([
					{ id: ["room", 10], body: "10" },
					{ id: ["room", 2], body: "2" },
				])
				read.remove("messages", ["room", 2])
				await server.commit(read)
				expect(await server.query({ collection: "messages" })).toHaveLength(3)
				expect(
					await db.query({
						sql: "select body from app_messages where conversation_id = 'room' and sequence = 2",
						parameters: [],
					}),
				).toEqual([])
			})

			test("rejects a collision on another unique column without changing its owner", async () => {
				await db.commit([
					{
						sql: "create unique index unique_task_text on app_tasks(task_text)",
						parameters: [],
					},
				])
				const first = server.transact()
				first.set("tasks", { id: "a", text: "Unique", done: false })
				await server.commit(first)
				const conflicting = server.transact()
				conflicting.set("tasks", { id: "b", text: "Unique", done: true })
				await expect(server.commit(conflicting)).rejects.toThrow()
				expect(await server.query({ collection: "tasks" })).toEqual([
					{ id: "a", text: "Unique", done: false },
				])
			})

			test("rolls back the whole batch and lets a failed push retry", async () => {
				const clientId = tag<ClientId>("retry")
				const request = {
					clientId,
					mutations: [
						{
							id: tag<MutationId>(1),
							ops: [
								{
									type: "set" as const,
									collection: "tasks" as const,
									value: { id: "a", text: "Valid", done: false },
								},
								{
									type: "set" as const,
									collection: "tasks" as const,
									value: { id: "b", text: "forbidden", done: false },
								},
							],
						},
					],
				}
				expect(await server.push(request)).toMatchObject({
					error: "unavailable",
				})
				expect(await server.query({ collection: "tasks" })).toEqual([])
				request.mutations[0].ops[1].value.text = "Allowed"
				expect(await server.push(request)).toEqual({ ok: true })
				expect(await server.query({ collection: "tasks" })).toHaveLength(2)
			})

			test("syncs two clients through existing tables and treats SQL values as parameters", async () => {
				const alice = new TandemClient({ schema, remote: server })
				const bob = new TandemClient({ schema, remote: server })
				const subscription = bob.subscribe({ collection: "tasks" }, () => {})
				const tx = alice.transact()
				const text = "Robert'); DROP TABLE app_tasks;--"
				tx.set("tasks", { id: "quoted'id", text, done: false })
				await alice.commit(tx)
				await bob.pullFromRemote()
				expect(bob.query({ collection: "tasks" })).toEqual([
					{ id: "quoted'id", text, done: false },
				])
				expect(
					await db.query({
						sql: "select task_text from app_tasks",
						parameters: [],
					}),
				).toEqual([{ task_text: text }])
				subscription.destroy()
				await alice.close()
				await bob.close()
			})

			test("reopens durable records and borrows the database lifetime", async () => {
				const tx = server.transact()
				tx.set("tasks", { id: "saved", text: "Durable record", done: false })
				await server.commit(tx)
				await server.close()
				const reopened = new TandemServer({
					schema,
					relations: {},
					storage: new TandemServerSqlStorage({ dialect, tables, ...db }),
				})
				expect(await reopened.query({ collection: "tasks" })).toEqual([
					{ id: "saved", text: "Durable record", done: false },
				])
				await reopened.close()
			})
		},
	)
}
