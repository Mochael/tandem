# Tandem documentation

The guides are also a Holocron site. Edit the Markdown files here; `docs.json` controls navigation and site settings. The homepage in `website/` remains a separate TanStack Start app.

## Run the docs site

From the repository root:

```sh
pnpm install
pnpm --filter @tandem/docs dev
```

The development server uses port 3100. Build and serve the production Node app with:

```sh
pnpm --filter @tandem/docs build
PORT=3100 pnpm --filter @tandem/docs start
```

Holocron serves the overview at `/overview`, the quickstart at `/quickstart`, and the remaining guides at their Markdown paths without `.md`. It also exposes `/llms.txt`, `/llms-full.txt`, and `/docs.zip`. Sidebar search filters page titles; it is not full-text search. The AI assistant is disabled.

The site runs on Vercel at [docs.tandemsync.dev](https://docs.tandemsync.dev). The `tandem-docs` project uses `docs` as its root directory, includes source files outside that directory, and runs Node.js 22. `vercel.json` sets the build command, serves static files, redirects `/` to `/overview`, and routes other requests to the built Holocron handler through `api/index.mjs`.

## Diagrams

The workspace applies `patches/@holocron.so__vite@0.38.0.patch` to render Holocron's Mermaid blocks with `beautiful-mermaid`. The patch covers both published JavaScript and source files; `pnpm-workspace.yaml` adds its runtime dependencies. It inherits the site theme through CSS variables, renders SVG on the server, and keeps wide diagrams horizontally scrollable on narrow screens. Invalid or unsupported diagrams display their source code. Review and regenerate the patch when upgrading Holocron.

## Guides

- [Overview](overview.md): what Tandem is and how the pieces fit together
- [Quickstart](quickstart.md): build a synced todo app with React
- [Queries](queries.md): query options, relations, and subscriptions
- [Server](server.md): run `TandemServer` and connect clients to it
- [SQL storage](sql.md): map collections onto existing SQL tables
- [Cloudflare](cloudflare.md): host sync with one Durable Object, D1, and SSE
- [Agent chat](agent-chat.md): stream an agent's reply to every viewer with plain subscriptions

## Concepts

- [How sync works](sync.md): mutations, push, pull, rebase, and live records
- [Custom remotes](custom-remote.md): implement the `RemoteApi` contract yourself

## Project

- [Roadmap](roadmap.md)
- [Relational queries design](design/relational-queries.md)
