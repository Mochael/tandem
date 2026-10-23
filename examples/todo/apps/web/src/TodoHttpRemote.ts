import type {
	TodoPullResult,
	TodoPushResult,
	TodoRemoteRequest,
	TodoSchema,
} from "@tandem/example-todo-shared"
import {
	connectTandemEventSource,
	type RemoteApi,
	type RemoteRequestError,
} from "@tanishqkancharla/tandem-core"
import * as errore from "errore"

class TodoHttpRemoteError extends errore.createTaggedError({
	name: "TodoHttpRemoteError",
	message: "Todo remote $operation request failed",
}) {}

async function request<Result>({
	operation,
	body,
}: {
	operation: "pull" | "push"
	body: TodoRemoteRequest
}): Promise<Result | RemoteRequestError> {
	const serialized = errore.try({
		try: () => JSON.stringify(body),
		catch: (cause) => new TodoHttpRemoteError({ operation, cause }),
	})
	if (serialized instanceof Error)
		return { error: "invalid-request", message: serialized.message }

	const response = await fetch("/api/tandem", {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: serialized,
	}).catch((cause) => new TodoHttpRemoteError({ operation, cause }))
	if (response instanceof Error)
		return { error: "unavailable", message: response.message }
	if (!response.ok) {
		return {
			error: response.status === 400 ? "invalid-request" : "unavailable",
			message: `HTTP ${response.status}`,
		}
	}

	const parsed = await response
		.json()
		.then((value: unknown) => ({ value }))
		.catch((cause) => new TodoHttpRemoteError({ operation, cause }))
	if (parsed instanceof Error)
		return { error: "unavailable", message: parsed.message }

	// This is the typed JSON boundary after the server validates the envelope.
	return parsed.value as Result
}

export class TodoHttpRemote implements RemoteApi<TodoSchema> {
	private poke: (() => Promise<void>) | undefined

	connect: RemoteApi<TodoSchema>["connect"] = ({ clientId, poke }) => {
		this.poke = poke
		const disconnect = connectTandemEventSource({
			url: `/api/tandem/events?clientId=${encodeURIComponent(clientId)}`,
			poke,
		})

		return Promise.resolve(() => {
			if (this.poke === poke) this.poke = undefined
			return disconnect()
		})
	}

	push: RemoteApi<TodoSchema>["push"] = async (args) => {
		const result = await request<TodoPushResult>({
			operation: "push",
			body: { action: "push", args },
		})
		if ("error" in result) return result

		// Pull now to acknowledge this client's mutation without waiting for the stream.
		void this.poke?.()
		return result
	}

	pull: RemoteApi<TodoSchema>["pull"] = async (args) => {
		const result = await request<TodoPullResult>({
			operation: "pull",
			body: { action: "pull", args },
		})
		return result
	}
}
