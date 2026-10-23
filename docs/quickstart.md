# Quickstart

This guide builds a todo app where a React client syncs through a `TandemServer`. The complete version is in [`examples/todo`](../examples/todo).

## Install

```sh
pnpm add @tanishqkancharla/tandem-core @tanishqkancharla/tandem-react
pnpm add @tanishqkancharla/tandem-server # server only
```

## 1. Define a schema

Share the schema between client and server.

```ts
// shared/schema.ts
import { collection, defineSchema, t } from "@tanishqkancharla/tandem-core"

export const schema = defineSchema({
	todos: collection({
		id: t.id(),
		text: t.string(),
		complete: t.boolean(),
		createdAt: t.number(),
	}),
})

export type Todo = {
	id: string
	text: string
	complete: boolean
	createdAt: number
}

export type TodoSchema = { todos: Todo }
```

Field builders are `t.id()`, `t.string()`, `t.number()`, and `t.boolean()`. To use compound IDs or custom storage encodings, see [Server](server.md#compound-ids).

## 2. Create a client

```ts
// client/db.ts
import {
	TandemClient,
	TandemClientIndexedDbStorage,
} from "@tanishqkancharla/tandem-core"
import { schema, type TodoSchema } from "../shared/schema"
import { todoRemote } from "./remote"

export const db = new TandemClient({
	schema,
	// Optional. Persists the local database across reloads.
	clientStorage: new TandemClientIndexedDbStorage<TodoSchema>({
		dbName: "todos",
		schema,
	}),
	// Optional. Without it, the client is a local-only database.
	remote: todoRemote,
	// Let the React provider manage the connection.
	autoConnect: false,
})
```

`db.ready` resolves once the client has loaded from `clientStorage`.

## 3. Read and write

Queries are plain objects. See [Queries](queries.md) for every option.

```ts
const open = db.query({
	collection: "todos",
	where: { complete: false },
	orderBy: { createdAt: "desc" },
})

const { result, destroy } = db.subscribe({ collection: "todos" }, (todos) => {
	console.log(todos)
})
destroy()
```

Writes go through transactions. `commit` applies the changes locally right away. The returned promise settles once the push to the remote finishes, and rejects if the push fails. When a push fails, the client rolls the changes back.

```ts
const tx = db.transact()
tx.set("todos", {
	id: crypto.randomUUID(),
	text: "Learn Tandem",
	complete: false,
	createdAt: Date.now(),
})
tx.update("todos", "todo-1", (todo) => ({ ...todo, complete: true }))
tx.remove("todos", "todo-2")
await db.commit(tx)
```

A transaction also exposes `get`, `list`, and `scan` for reads that should see its own uncommitted writes.

## 4. Use it in React

`TandemClientProvider` waits for `db.ready`, and connects to the remote when you pass `connect`.

```tsx
// client/main.tsx
import { TandemClientProvider } from "@tanishqkancharla/tandem-react"
import { createRoot } from "react-dom/client"
import { App } from "./App"
import { db } from "./db"

createRoot(document.getElementById("root")!).render(
	<TandemClientProvider client={db} connect>
		<App />
	</TandemClientProvider>,
)
```

Bind the hooks to your schema once, then use them in components.

```tsx
// client/App.tsx
import {
	useTandemQuery,
	useTandemTransaction,
	type UseTandemQuery,
	type UseTandemTransaction,
} from "@tanishqkancharla/tandem-react"
import type { TodoSchema } from "../shared/schema"

const useQuery: UseTandemQuery<TodoSchema> = useTandemQuery
const useTransaction: UseTandemTransaction<TodoSchema> = useTandemTransaction

export function App() {
	const todos =
		useQuery({ collection: "todos", orderBy: { createdAt: "desc" } }) ?? []

	const addTodo = useTransaction((tx, text: string) => {
		tx.set("todos", {
			id: crypto.randomUUID(),
			text,
			complete: false,
			createdAt: Date.now(),
		})
	})

	const toggleTodo = useTransaction((tx, id: string) => {
		tx.update("todos", id, (todo) => ({ ...todo, complete: !todo.complete }))
	})

	return (
		<div>
			{todos.map((todo) => (
				<label key={todo.id}>
					<input
						type="checkbox"
						checked={todo.complete}
						onChange={() => toggleTodo(todo.id)}
					/>
					{todo.text}
				</label>
			))}
			<button onClick={() => addTodo("New todo")}>Add</button>
		</div>
	)
}
```

The package also exports `useTandemClient` for direct client access and `useEntity(collection, id)` for reading a single record.

## 5. Add a server

Run a `TandemServer` and expose its `push` and `pull` methods over HTTP.

```ts
// server/index.ts
import {
	TandemServer,
	TandemServerJsonFileStorage,
} from "@tanishqkancharla/tandem-server"
import { schema, type TodoSchema } from "../shared/schema"

const server = new TandemServer({
	schema,
	relations: {},
	storage: new TandemServerJsonFileStorage<TodoSchema>({
		filePath: "./data/tandem.json",
	}),
})

// In your HTTP framework:
// POST /api/tandem { action: "push", args } -> await server.push(args)
// POST /api/tandem { action: "pull", args } -> await server.pull(args)
// GET /api/tandem/events?clientId=... -> return the response below
```

The events route streams pull hints to the browser as server-sent events. It returns a standard `Response`, so fetch-style frameworks such as Hono can return it directly:

```ts
import { tag, type ClientId } from "@tanishqkancharla/tandem-core"
import { createTandemEventStreamResponse } from "@tanishqkancharla/tandem-server"

function events(request: Request) {
	const clientId = new URL(request.url).searchParams.get("clientId")
	if (!clientId) return new Response("Missing clientId", { status: 400 })
	return createTandemEventStreamResponse({
		remote: server,
		clientId: tag<ClientId>(clientId),
		signal: request.signal,
	})
}
```

On the client, implement `RemoteApi` by forwarding each call to that endpoint.

```ts
// client/remote.ts
import {
	connectTandemEventSource,
	type RemoteApi,
} from "@tanishqkancharla/tandem-core"
import type { TodoSchema } from "../shared/schema"

async function call(action: "push" | "pull", args: unknown) {
	const response = await fetch("/api/tandem", {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify({ action, args }),
	})
	if (!response.ok) throw new Error(`Tandem ${action} failed`)
	return response.json()
}

export const todoRemote: RemoteApi<TodoSchema> = {
	async connect({ clientId, poke }) {
		return connectTandemEventSource({
			url: `/api/tandem/events?clientId=${encodeURIComponent(clientId)}`,
			poke,
		})
	},
	async push(args) {
		await call("push", args)
	},
	pull: (args) => call("pull", args),
}
```

`connectTandemEventSource` pulls when the stream opens or reconnects, on every server poke, and every 60 seconds as a fallback (`pullInterval`). The browser's `EventSource` reconnects dropped streams on its own.

The client only pulls data for queries it subscribes to, so another client's writes appear once a matching `subscribe` or `useTandemQuery` is active.

## Next steps

- [Queries](queries.md)
- [Server](server.md)
- [How sync works](sync.md)
