# Tandem website

TanStack Start + React + Maui. The home page lives at `/`; documentation lives at `/docs` and `/docs/<page>`.

The homepage runs at [tandemsync.dev](https://tandemsync.dev). Its Docs link opens the Holocron site at [docs.tandemsync.dev](https://docs.tandemsync.dev). Its main button copies a prompt that tells a coding agent to follow [`public/setup.md`](public/setup.md), served at [tandemsync.dev/setup.md](https://tandemsync.dev/setup.md).

Run from the repository root:

```sh
pnpm install
pnpm --filter @tandem/website dev
pnpm --filter @tandem/website build
pnpm --filter @tandem/website preview
pnpm --filter @tandem/website type-check
pnpm --filter @tandem/website lint
pnpm --filter @tandem/website test:e2e
```

The development and preview servers use port 3000. In an Amp orb, expose the development server with:

```sh
amp orb service start tandem-website --command 'pnpm --filter @tandem/website dev' --port 3000 --portal
```

## Documentation

The website imports Markdown directly from `docs/`; edit those files rather than copying content into the website. `src/documents.ts` defines the sidebar order and page titles. Relative Markdown links resolve to website pages, and repository links resolve to GitHub. GFM tables, heading anchors, highlighted code, and Mermaid diagrams are supported.

```tsx
const document = await loadDocument(slug)
return <DocPage document={document} />
```

## Rendering and hosting

Maui 0.0.62 reads `window.matchMedia` during provider initialization. The site therefore uses TanStack Start's SPA mode and mounts Maui inside `ClientOnly`. Page content requires JavaScript; the build prerenders a static shell, not the full documentation. Full SSR and prerendered page content need SSR-compatible Maui providers.

`pnpm --filter @tandem/website build` writes static files to `website/dist/client`. Serve existing assets normally and rewrite other paths to `/_shell.html` so documentation deep links work. `website/vercel.json` supplies this configuration. The Vercel project `tandem-website` uses `website` as its root directory, includes files outside that directory (the shared `docs/`), and runs Node.js 22.
