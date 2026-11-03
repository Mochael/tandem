# Server

`TandemServer` from `@tanishqkancharla/tandem-server` is a server-side Tandem database. It implements `RemoteApi`, so clients can sync against it, and it also exposes its own query and transaction API for server code.

## Create a server

`TandemServer` takes the same runtime schema and relations as the client, plus a storage adapter.

```ts
import {
	TandemServer,
	TandemServerJsonFileStorage,
} from "@tanishqkancharla/tandem-server"
import { schema, type TodoSchema } from "./schema"

const server = new TandemServer({
	schema,
	relations: {},
	storage: new TandemServerJsonFileStorage<TodoSchema>({
		filePath: "./data/tandem.json",
	}),
})
```

Call `server.close()` on shutdown to close storage.

Register shared mutators once in the constructor with `mutators`; optional `hooks` add server-only authorization and post-commit work. `server.mutate.name(args)` retains each registered mutation's argument type. Pass authenticated request context to `server.push(request, { context })`; the hooks factory receives it for that request. See [Agent chat](agent-chat.md) for the full pattern.

## Connect clients

`TandemServer` has `connect`, `push`, and `pull` methods that match `RemoteApi`. Clients in the same process can use the server as their `remote` directly:

```ts
const client = new TandemClient({ schema, remote: server })
```

Across a network, the client's `remote` is a transport that forwards `push` and `pull` to the server and calls `poke` when there may be new data. The [Quickstart](quickstart.md#5-add-a-server) shows a minimal HTTP transport, and [`examples/todo`](https://github.com/tanishqkancharla/tandem/tree/main/examples/todo) has a complete Hono server.

After every commit that writes data, the server pokes each connected client whose view the commit changed. A client's view is the scan window of its latest pull made while connected, so the first view comes from the pull an event stream's `ready` starts. The poke carries the commit's revision and the changed records inside that view, so the client shows them before it pulls:

- `set` holds each changed record in the view, in full, even after a partial `update`.
- `remove` holds records the client already has that the commit deleted or moved out of its view.
- `append` holds text inferred from a committed string extension to a record the client already has, instead of the whole record.

A client's first payload goes out at once. Payloads from further commits within `livePokeInterval` (50 ms by default) merge into one sent when the interval ends: each record appears once, at its latest value, and consecutive appends to a field join into one. A burst of commits, such as a reply streamed token by token, therefore costs each viewer at most one payload per interval. Pass `livePokeInterval: 0` to send each commit's payload on its own.

Pokes never carry a record, or name a record ID, that a pull with the same window would not return. Relation includes, limits, and offsets are evaluated the way a pull evaluates them. A record that enters the view without changing, such as the next record under a `limit`, arrives with the pull that every poke still starts. A client that has not pulled yet gets a bare poke, and the client whose push made the commit always gets one, so it pulls the acknowledgement at once instead of waiting for a stream of payloads to go quiet. Bare pokes are never delayed; a client's merged payload goes out right before one.

Every pull reports the revision its read reflects. Revisions count this instance's commits and start over in a new instance, like cookies.

`createTandemEventStreamResponse` serves those pokes to browsers as server-sent events, and `connectTandemEventSource` from `@tanishqkancharla/tandem-core` turns them back into `poke` calls. A poke event's data is the payload as JSON, or empty for a bare poke; pass `schema` to both helpers to apply collection codecs. A stream whose reader falls behind drops payloads and sends one bare poke when it catches up. The stream sends `ready` after it registers, so the client pulls once per connection and catches writes it missed while disconnected. It also sends a keep-alive comment every 15 seconds and closes when the request aborts or the body is cancelled. Open streams keep an HTTP server from closing, so combine the request signal with a shutdown signal, as [`examples/todo`](../examples/todo/apps/server/src/app.ts) does. The [Quickstart](quickstart.md#5-add-a-server) shows both sides.

## Read and write on the server

Queries use the same object shape as the client (see [Queries](queries.md)), but return promises.

```ts
const todos = await server.query({
	collection: "todos",
	where: { complete: false },
})

const { result, destroy } = await server.subscribe(
	{ collection: "todos" },
	(todos) => console.log(todos),
	{ onError: (error) => console.error(error) },
)
```

Server transactions have async reads and sync writes. Commit them through the server so connected clients get poked.

```ts
const tx = server.transact()
const existing = await tx.get("todos", "welcome")
if (!existing) {
	tx.set("todos", {
		id: "welcome",
		text: "Build something",
		complete: false,
		createdAt: Date.now(),
	})
}
await server.commit(tx)
```

A transaction also has `list`, `scan`, `update`, `remove`, `query`, and `cancel`.

`update` accepts partial assignments and awaits the current record. Reads in the same transaction see those assignments; missing records and empty payloads are no-ops. Supplied nested objects and arrays replace their entire field. Use `set` for complete replacements; updates cannot change identity or delete fields.

```tsx
const tx = server.transact()
await tx.update("todos", "welcome", { complete: true })
await server.commit(tx)
```

### Append text

Define a shared named mutator that reads the record and writes its extended value. The client runs it optimistically; the server runs the same code in its serialized mutation queue.

```ts
const appendReply = defineMutator(schema, {
  args: z.object({ id: z.string(), text: z.string() }),
  run: async (tx, { id, text }) => {
    const message = await tx.get("messages", id)
    if (message) tx.set("messages", { ...message, text: message.text + text })
  },
})
```

A missing record stays missing. A nullable string field can use `(message.note ?? "") + text`. The app's argument schema should reject malformed text. Concurrent named calls each read the latest committed value in the server queue. Raw transactions whose reads ran before another commit do not gain that guarantee.

The server compares each committed record with its before image. If the only differences are string extensions whose suffixes round trip through the collection codec, a viewer that already has the record receives compact append pokes. Other changes send a full record. A poke carries each field's previous length and a guarded previous revision; missing or reordered pokes fall back to a pull. The server tracks the last write of its 10,000 most recently written records; older entries use a conservative guard.

Pending client mutations replay their recorded set operations during rebase, so an optimistic append may temporarily cover newer live text until its confirming pull. The server's named mutation result is authoritative.

## Compound IDs

A record ID can be a tuple. Tandem stores its parts in order, and `scan` accepts a prefix of them.

```ts
import { collection, defineSchema } from "@tanishqkancharla/tandem-core"

type Entry = {
	id: readonly [sessionId: string, sequence: number]
	body: string
}

const schema = defineSchema({
	entries: collection<Entry>({ fields: ["id", "body"] }),
})

const tx = server.transact()
const sessionEntries = await tx.scan("entries", { prefix: ["session-1"] })
```

Client transactions support the same `scan` call.

## Storage

Storage adapters implement `TandemServerStorageApi`:

```ts
interface TandemServerStorageApi<Schema> {
	scan(args?: ScanStorageArgs): Promise<TandemTuple<Schema>[]>
	commit(writes: WriteOps<TandemTuple<Schema>>): Promise<void>
	close(): Promise<void>
}
```

`TandemServerJsonFileStorage` keeps tuples in memory and writes the whole file on each commit. It suits development and examples, not production.

For existing SQL tables, see the experimental [SQL adapter](sql.md) and [Cloudflare D1 guide](cloudflare.md). The SQL adapter keeps acknowledgements in memory until durable metadata support is added.

## Limitations

- Records and acknowledged mutation IDs persist when the storage adapter persists them. The JSON-file adapter stores both; the experimental SQL adapter persists only application records. Revisions, cookies, and client scan windows live in the server process and reset on restart.
- Pokes come from the `TandemServer` instance that committed the write, or from storage `subscribe` notifications. Writes made by other processes or directly to the database do not poke clients unless storage reports them. Event streams connected to another process miss those pokes and rely on the client's fallback pull. Storage notifications are bare pokes, without records.
- Appends assume the server knows every write to a record. When storage reports another process's write through `subscribe`, the server makes appends wait for a pull read after it. A write that storage does not report can let a client extend text that another process replaced, until its next pull.
- Revisions are per instance. If a client's pokes and pulls reach different instances, live records shown from one instance's pokes are dropped by the next pull from another.
- The server does no authentication or authorization. Check requests in your transport before calling `push` or `pull`, and authorize `connect` (including the event stream route) the same way: pokes carry the records of the client's latest pulled view to whoever holds its connection. A view lasts until the client's next pull or until its last connection closes, so when a user loses access, close that client's connections: pokes then carry no records until a pull your transport authorizes.
