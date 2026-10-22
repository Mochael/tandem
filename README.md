# Tandem

A sync engine and database for building collaborative apps.

> [!WARNING]
> Tandem is a work in progress and is not ready for production use. APIs change without notice.

Tandem gives each client a local, queryable database. Writes apply locally first. Tandem then syncs them to a server in the background and replays any pending local changes on top of what the server sends back.

## Features

- Optimistic local writes with rollback when the server rejects them
- Typed object queries with `select`, `where`, `orderBy`, `limit`, `offset`, and relations through `with`
- Live query subscriptions that re-run when matching data changes
- Optional IndexedDB persistence on the client, shared safely between browser tabs
- Scalar or compound tuple IDs, with typed prefix scans
- A server database that implements the sync protocol
- React bindings

## Packages

| Package                                               | Description                                        |
| ----------------------------------------------------- | -------------------------------------------------- |
| [`@tanishqkancharla/tandem-core`](packages/core)      | Client database, schema, queries, and sync engine  |
| [`@tanishqkancharla/tandem-server`](packages/server)  | Server database and storage adapters               |
| [`@tanishqkancharla/tandem-react`](packages/react)    | React provider and hooks                           |
| [`@tanishqkancharla/gatekeeper`](packages/gatekeeper) | Test harness for controlling call order and faults |

## Installation

```sh
pnpm add @tanishqkancharla/tandem-core
pnpm add @tanishqkancharla/tandem-react # React bindings
pnpm add @tanishqkancharla/tandem-server # Server
```

## Example

```ts
import {
	collection,
	defineSchema,
	t,
	TandemClient,
} from "@tanishqkancharla/tandem-core"

const schema = defineSchema({
	todos: collection({
		id: t.id(),
		text: t.string(),
		complete: t.boolean(),
	}),
})

const client = new TandemClient({ schema })
await client.ready

const tx = client.transact()
tx.set("todos", { id: "1", text: "Try Tandem", complete: false })
await client.commit(tx)

const { result, destroy } = client.subscribe(
	{ collection: "todos", where: { complete: false } },
	(todos) => console.log(todos),
)
```

To sync between clients, pass a `remote`. See [Quickstart](docs/quickstart.md).

## Documentation

- [Overview](docs/overview.md): what Tandem is and how the pieces fit together
- [Quickstart](docs/quickstart.md): build a synced todo app
- [Queries](docs/queries.md): query options and relations
- [Server](docs/server.md): run `TandemServer` and connect clients to it
- [How sync works](docs/sync.md): mutations, push, pull, and rebase
- [Custom remotes](docs/custom-remote.md): implement the `RemoteApi` contract yourself
- [Roadmap](docs/roadmap.md)

The full index is in [docs/README.md](docs/README.md).

## Examples

[`examples/todo`](examples/todo) is a React todo app backed by a Hono server that runs `TandemServer` with JSON file storage.

```sh
pnpm install
pnpm exec turbo run dev --filter=@tandem/example-todo-web --filter=@tandem/example-todo-server
```

## Development

This is a pnpm and Turborepo monorepo.

```sh
pnpm install
pnpm build
pnpm test
pnpm type-check
pnpm lint
pnpm format
```

`pnpm dst:run` runs the deterministic simulation tests in [`dst`](dst). See [`specs/deterministic-simulation-testing.md`](specs/deterministic-simulation-testing.md).

## License

[MIT](LICENSE.md)
