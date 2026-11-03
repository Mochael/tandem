import { defineConfig } from "vitest/config"

export default defineConfig({
	oxc: {
		target: "es2022",
	},
	resolve: {
		alias: {
			"@tanishqkancharla/tandem-server/sql": new URL(
				"./packages/server/src/storage/TandemServerSqlStorage.ts",
				import.meta.url,
			).pathname,
			"@tanishqkancharla/tandem-core/internal": new URL(
				"./packages/core/src/internal.ts",
				import.meta.url,
			).pathname,
			"@tanishqkancharla/gatekeeper": new URL(
				"./packages/gatekeeper/src/Gatekeeper.ts",
				import.meta.url,
			).pathname,
			"@tanishqkancharla/tandem-core": new URL(
				"./packages/core/src/index.ts",
				import.meta.url,
			).pathname,
			"@tanishqkancharla/tandem-server": new URL(
				"./packages/server/src/index.ts",
				import.meta.url,
			).pathname,
			"@tanishqkancharla/tandem-react": new URL(
				"./packages/react/src/index.ts",
				import.meta.url,
			).pathname,
		},
	},
	test: {
		allowOnly: !process.env.CI,
		// Node 22 ships EventSource behind a flag; SSE tests use the real one.
		execArgv: ["--experimental-eventsource"],
		isolate: false,
	},
})
