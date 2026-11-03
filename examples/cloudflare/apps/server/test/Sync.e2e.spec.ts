import fs from "node:fs/promises"
import { TandemClient, Logger } from "@tanishqkancharla/tandem-core"
import * as errore from "errore"
import { build } from "esbuild"
import { Miniflare } from "miniflare"
import { expect, test } from "vitest"
import { schema } from "../src/schema.js"
import { createRemote } from "../src/remote.js"

test("two clients sync existing D1 rows over SSE and recover a disconnected view", async () => {
	const bundle = await build({
		entryPoints: ["src/index.ts"],
		bundle: true,
		write: false,
		format: "esm",
		platform: "neutral",
		mainFields: ["module", "main"],
		conditions: ["workerd", "worker"],
		external: ["node:*"],
	})
	const runtime = new Miniflare({
		modules: true,
		script: bundle.outputFiles[0].text,
		compatibilityDate: "2026-07-01",
		compatibilityFlags: ["nodejs_compat"],
		d1Databases: { DB: "cloudflare-example" },
		durableObjects: { SYNC: { className: "SyncObject", useSQLite: true } },
	})
	await using cleanup = new errore.AsyncDisposableStack()
	cleanup.defer(() => runtime.dispose())
	const db = await runtime.getD1Database("DB")
	await db.prepare(await fs.readFile("schema.sql", "utf8")).run()
	await db
		.prepare(
			"insert into tasks (task_id, title, is_done) values ('existing', 'From SQL', 0)",
		)
		.run()
	const url = (await runtime.ready).toString()
	const alice = new TandemClient({
		schema,
		remote: createRemote(url),
		logger: new Logger(),
	})
	const bob = new TandemClient({
		schema,
		remote: createRemote(url),
		logger: new Logger(),
	})
	cleanup.defer(() => alice.close())
	cleanup.defer(() => bob.close())
	alice.subscribe({ collection: "tasks" }, () => {})
	bob.subscribe({ collection: "tasks" }, () => {})
	await expect
		.poll(() => bob.query({ collection: "tasks" }))
		.toEqual([{ id: "existing", text: "From SQL", done: false }])
	const update = alice.transact()
	update.set("tasks", {
		id: "existing",
		text: "Edited through Tandem",
		done: true,
	})
	await alice.commit(update)
	await expect
		.poll(() => bob.query({ collection: "tasks" }))
		.toEqual([{ id: "existing", text: "Edited through Tandem", done: true }])
	expect(
		await db.prepare("select title, is_done from tasks").all(),
	).toMatchObject({
		results: [{ title: "Edited through Tandem", is_done: 1 }],
	})
	await bob.disconnect()
	const remove = alice.transact()
	remove.remove("tasks", "existing")
	await alice.commit(remove)
	await bob.connect()
	await expect.poll(() => bob.query({ collection: "tasks" })).toEqual([])

	const malformed = await fetch(new URL("/api/tandem", url), {
		method: "POST",
		body: "not JSON",
	})
	expect(malformed.status).toBe(400)
}, 30_000)
