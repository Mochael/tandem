import { serve } from "@hono/node-server"
import * as errore from "errore"
import { fileURLToPath } from "node:url"
import { createTodoApp } from "./app.js"

class TodoServerStartupError extends errore.createTaggedError({
	name: "TodoServerStartupError",
	message: "Todo server failed to $operation",
}) {}

function getPort() {
	const port = Number(process.env.PORT ?? 8787)
	if (Number.isInteger(port) && port > 0) return port
	return new TodoServerStartupError({ operation: "read PORT" })
}

async function start() {
	const port = getPort()
	if (port instanceof Error) return port

	const filePath =
		process.env.TANDEM_TODO_DATA_FILE ??
		fileURLToPath(new URL("../.data/tandem.json", import.meta.url))
	const service = await createTodoApp({ filePath })
	if (service instanceof Error) {
		return new TodoServerStartupError({
			operation: "open TandemServer",
			cause: service,
		})
	}

	const nodeServer = errore.try({
		try: () => serve({ fetch: service.app.fetch, port }),
		catch: (cause) =>
			new TodoServerStartupError({ operation: "listen", cause }),
	})
	if (nodeServer instanceof Error) {
		const closed = await service
			.close()
			.catch(
				(cause) => new TodoServerStartupError({ operation: "close", cause }),
			)
		if (closed instanceof Error) console.error(closed)
		return nodeServer
	}
	const listening = await new Promise<TodoServerStartupError | undefined>(
		(resolve) => {
			const onListening = () => {
				nodeServer.off("error", onError)
				resolve(undefined)
			}
			const onError = (cause: Error) => {
				nodeServer.off("listening", onListening)
				resolve(new TodoServerStartupError({ operation: "listen", cause }))
			}
			nodeServer.once("listening", onListening)
			nodeServer.once("error", onError)
		},
	)
	if (listening instanceof Error) {
		const closed = await service
			.close()
			.catch(
				(cause) => new TodoServerStartupError({ operation: "close", cause }),
			)
		if (closed instanceof Error) console.error(closed)
		return listening
	}
	console.log(`Todo server listening on http://localhost:${port}`)

	const shutdown = () => {
		nodeServer.close((cause) => {
			if (cause)
				console.error(new TodoServerStartupError({ operation: "stop", cause }))
			void service
				.close()
				.catch(
					(cause) => new TodoServerStartupError({ operation: "close", cause }),
				)
				.then((closed) => {
					if (closed instanceof Error) console.error(closed)
				})
		})
		service.endEventStreams()
	}

	process.once("SIGINT", shutdown)
	process.once("SIGTERM", shutdown)
}

const started = await start()
if (started instanceof Error) {
	console.error(started)
	process.exitCode = 1
}
