import { expectTypeOf } from "vitest"
import type { TandemServerStorageApi } from "../src/index.js"
import {
	TandemServerSqlStorage,
	type TandemSqlTable,
} from "../src/storage/TandemServerSqlStorage.js"

type Schema = {
	users: { id: string; name: string }
	entries: { id: readonly [string, number]; body: string }
}

declare const users: TandemSqlTable<Schema["users"]>
declare const entries: TandemSqlTable<Schema["entries"]>
expectTypeOf(users.encode).parameter(0).toEqualTypeOf<Schema["users"]>()
expectTypeOf(entries.decode).returns.toEqualTypeOf<Schema["entries"]>()
expectTypeOf(entries.idColumns).toEqualTypeOf<readonly [string, string]>()

const invalid: TandemSqlTable<Schema["entries"]> = {
	table: "entries",
	// @ts-expect-error Both parts of a compound ID need SQL columns.
	idColumns: ["id"],
	encode: ({ body }) => ({ body }),
	decode: entries.decode,
}
void invalid

const storage = new TandemServerSqlStorage<Schema>({
	dialect: "sqlite",
	tables: { users, entries },
	query: async () => [],
	commit: async () => {},
})
expectTypeOf(storage).toMatchTypeOf<TandemServerStorageApi<Schema>>()
