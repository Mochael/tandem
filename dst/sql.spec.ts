import { PGlite } from "@electric-sql/pglite"
import {
	TandemServerSqlStorage,
	type TandemServerSqlStorageArgs,
	type TandemSqlRow,
} from "@tanishqkancharla/tandem-server/sql"
import { expect, test } from "vitest"
import {
	formatArtifact,
	memorySink,
	parseArtifact,
	replay,
} from "./DstReplay.js"
import { DstSimulation } from "./DstSimulation.js"
import type { DstSchema } from "./DstWorld.js"

const tables: TandemServerSqlStorageArgs<DstSchema>["tables"] = {
	projects: {
		table: "projects",
		idColumns: ["id"],
		encode: ({ name }) => ({ name }),
		decode: (row) => row as DstSchema["projects"],
	},
	tasks: {
		table: "tasks",
		idColumns: ["id"],
		encode: ({ projectId, text, note, done, priority }) => ({
			projectId,
			text,
			note: note ?? null,
			done,
			priority,
		}),
		decode: (row) => {
			const { note, ...rest } = row
			return {
				...rest,
				...(note === null ? {} : { note }),
			} as DstSchema["tasks"]
		},
	},
	comments: {
		table: "comments",
		idColumns: ["id"],
		encode: ({ taskId, body }) => ({ taskId, body }),
		decode: (row) => row as DstSchema["comments"],
	},
	memberships: {
		table: "memberships",
		idColumns: ["project_id", "member_id"],
		encode: ({ role }) => ({ role }),
		decode: (row) => ({
			id: [row.project_id as string, row.member_id as string],
			role: row.role as string,
		}),
	},
}

test.each([3, 7, 19, 42, 81])(
	"SQL storage survives network faults and client crashes, seed %i",
	async (seed) => {
		const db = new PGlite()
		await db.exec(`
		create table projects (id text primary key, name text);
		create table tasks (id text primary key, "projectId" text, text text, note text, done boolean, priority integer);
		create table comments (id text primary key, "taskId" text, body text);
		create table memberships (project_id text, member_id text, role text, primary key (project_id, member_id));
	`)
		const storage = () =>
			new TandemServerSqlStorage<DstSchema>({
				dialect: "postgres",
				tables,
				query: async ({ sql, parameters }) =>
					(await db.query<TandemSqlRow>(sql, [...parameters])).rows,
				commit: async (statements) => {
					await db.transaction(async (tx) => {
						for (const { sql, parameters } of statements)
							await tx.query(sql, [...parameters])
					})
				},
			})
		const sink = memorySink()
		const result = await new DstSimulation({
			seed,
			steps: 1000,
			faultRate: 0.15,
			crashRate: 0.02,
			namedAppendRate: 0.15,
		}).execute(sink, storage())
		expect(result.violation).toBeUndefined()
		for (const [name, state] of Object.entries(result.states))
			if (name !== "server") expect(state).toEqual(result.states.server)
		expect(result.trace.some((event) => event.type === "crash")).toBe(true)
		expect(result.trace.some((event) => event.type === "drop")).toBe(true)
		expect(result.trace.some((event) => event.type === "mutate")).toBe(true)
		await db.exec("truncate projects, tasks, comments, memberships")
		const replayed = await replay(
			parseArtifact(formatArtifact(sink.lines)),
			storage(),
		)
		expect(replayed.divergence).toBeUndefined()
		expect(replayed.violation).toBeUndefined()
		await db.close()
	},
	30_000,
)
