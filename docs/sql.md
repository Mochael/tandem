# Existing SQL tables

`TandemServerSqlStorage` maps Tandem collections onto your application's existing tables. Choose `sqlite`, `postgres`, or `mysql` for SQL syntax, and supply your database's query and atomic commit functions. The adapter uses Kysely to compile SQL without loading a driver, ORM, or migration system.

This adapter is experimental. Application records persist in SQL, but mutation acknowledgements live in the adapter's memory. After a server restart, a pending mutation may replay or a client may receive a mutation-gap error. Reconnecting SSE restores subscriptions, not acknowledgements. Durable sync metadata is separate work; do not use this adapter where restart-safe writes are required yet.

## Map a collection

Suppose your application already has this table:

```sql
CREATE TABLE tasks (
  task_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  is_done INTEGER NOT NULL DEFAULT 0,
  audit_note TEXT
);
```

Map it to a collection with a scalar ID:

```ts
import type { TandemSqlTable } from "@tanishqkancharla/tandem-server/sql"

type Task = { id: string; text: string; done: boolean }

export const tasksTable: TandemSqlTable<Task> = {
  table: "tasks",
  idColumns: ["task_id"],
  encode: ({ text, done }) => ({ title: text, is_done: Number(done) }),
  decode: (row) => ({
    id: row.task_id as string,
    text: row.title as string,
    done: row.is_done === 1,
  }),
}
```

`encode` returns SQL column names and values. The adapter supplies the primary-key values from Tandem's record ID. `decode` receives a SQL row and returns the complete Tandem record, including its ID. Use these functions to convert booleans, dates, JSON, and other driver-specific values. Validate returned rows here if your database is not already enforcing their shape.

Updates change only mapped columns. In this example, `audit_note` stays under the application's control. New rows still need values or database defaults for every required unmapped column. The adapter neither creates nor alters tables.

Use primary keys whose equality matches your application's record identity; for string IDs, avoid case-insensitive key collations if differently cased IDs are distinct records. Database constraints still apply, including foreign keys and unique columns. Batches remove records before setting records, in the order provided by Tandem; they do not sort writes by foreign-key dependency.

## Supply database execution

The adapter accepts two functions:

```ts
import {
  TandemServerSqlStorage,
  type TandemServerSqlStorageArgs,
} from "@tanishqkancharla/tandem-server/sql"

type AppSchema = { tasks: Task }

export function createStorage(
  database: Pick<TandemServerSqlStorageArgs<AppSchema>, "query" | "commit">,
) {
  return new TandemServerSqlStorage<AppSchema>({
    dialect: "sqlite",
    tables: { tasks: tasksTable },
    ...database,
  })
}
```

Each statement contains `sql` and `parameters`. Pass the parameters through the driver's binding API. Do not interpolate them into SQL strings.

- `query(statement)` returns an array of SQL rows.
- `commit(statements)` executes the whole batch atomically. If any statement fails, it must reject and roll back every statement. Executing each statement independently is not sufficient.

For D1, use `prepare().bind().all()` for reads and `batch()` for commits. See the [Cloudflare guide](cloudflare.md) for a complete setup. PostgreSQL and MySQL hosts can use their existing driver's transaction API on one connection. Choosing a dialect changes SQL syntax; it does not open a connection or add a driver.

Pass the adapter as `storage` to `TandemServer`, with the same runtime schema used by clients. The application owns the database connection: `server.close()` does not close that connection.

## Compound IDs

List primary-key columns in the same order as the ID parts:

```ts
type Message = {
  id: readonly [conversationId: string, sequence: number]
  body: string
}

const messagesTable: TandemSqlTable<Message> = {
  table: "messages",
  idColumns: ["conversation_id", "sequence"],
  encode: ({ body }) => ({ body }),
  decode: (row) => ({
    id: [row.conversation_id as string, row.sequence as number],
    body: row.body as string,
  }),
}
```

Internally, Tandem addresses a message with a tuple key such as `["record", "messages", "room-1", 42]`. The adapter translates that key to the table and its primary-key columns. It does not store the key array in SQL or copy records into another table.

## Current scope

Run writes to synced records through one `TandemServer`, including writes from backend jobs. Multiple servers sharing the same database do not share this adapter's acknowledgements or mutation queue. Direct SQL changes become visible on the next pull, but this adapter does not notify connected clients about them.

Scans read current SQL rows. Shared ID prefixes become SQL equality constraints; remaining bounds, sorting, and limits run in memory over the decoded rows for that scan. General filters and relation queries use Tandem's existing query engine rather than SQL joins or predicate pushdown. Large collections can therefore be expensive even when a query returns few records.

Supported dialects are exercised against D1's local runtime, PostgreSQL through PGlite, and an optional MySQL integration suite. They share table mappings and query construction, while the host supplies each driver's execution and transaction behavior.

## References

- [Kysely query compilation](https://www.kysely.dev/docs/recipes/splitting-query-building-and-execution)
- [D1 batches](https://developers.cloudflare.com/d1/worker-api/d1-database/#batch)
