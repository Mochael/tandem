import type {
	AnySchema,
	ClientId,
	RemoteApi,
} from "@tanishqkancharla/tandem-core"
import * as errore from "errore"
import { TandemServerError } from "./TandemServerError.js"

export type TandemEventStreamArgs = {
	/** Registers the stream for pokes, such as a `TandemServer` or a wrapper around one. */
	remote: Pick<RemoteApi<AnySchema>, "connect">
	clientId: ClientId
	/** Aborts when the request ends. Cancelling the response body also closes the stream. */
	signal?: AbortSignal
	/** Milliseconds between keep-alive comments. Defaults to 15 seconds. */
	heartbeatInterval?: number
}

const encoder = new TextEncoder()
const readyFrame = encoder.encode("event: ready\ndata:\n\n")
const pokeFrame = encoder.encode("event: poke\ndata:\n\n")
const heartbeatFrame = encoder.encode(": keep-alive\n\n")

/**
 * Streams data-free pull hints to one client as server-sent events.
 *
 * Sends `ready` after the poke callback is registered and `poke` whenever
 * committed data may have changed. Clients pull over the normal sync
 * transport in response.
 */
export async function createTandemEventStreamResponse({
	remote,
	clientId,
	signal,
	heartbeatInterval = 15_000,
}: TandemEventStreamArgs): Promise<Response> {
	let controller!: ReadableStreamDefaultController<Uint8Array>
	let closed = false
	// A reader that falls behind gets one pending poke, not one frame per commit.
	let pokePending = false

	const enqueue = (frame: Uint8Array) => {
		if (!closed) controller.enqueue(frame)
	}
	const poke = () => {
		if (controller.desiredSize !== null && controller.desiredSize > 0) {
			enqueue(pokeFrame)
		} else {
			pokePending = true
		}
		return Promise.resolve()
	}

	const body = new ReadableStream<Uint8Array>({
		start: (streamController) => {
			controller = streamController
		},
		pull: () => {
			if (!pokePending) return
			pokePending = false
			enqueue(pokeFrame)
		},
		cancel: () => close(),
	})

	const connected = await remote
		.connect({ clientId, poke })
		.catch((cause) => new TandemServerError({ operation: "connect", cause }))
	if (connected instanceof Error || typeof connected !== "function") {
		return Response.json(
			{ error: "unavailable", message: connected.message },
			{ status: 503 },
		)
	}

	const heartbeat = setInterval(
		() => enqueue(heartbeatFrame),
		heartbeatInterval,
	)
	const close = () => {
		if (closed) return
		closed = true
		clearInterval(heartbeat)
		signal?.removeEventListener("abort", onAbort)
		// The consumer may have cancelled the body already.
		errore.try(() => controller.close())
		void connected().catch((cause: unknown) => {
			console.error(new TandemServerError({ operation: "disconnect", cause }))
		})
	}
	const onAbort = () => close()
	if (signal?.aborted) close()
	else {
		signal?.addEventListener("abort", onAbort, { once: true })
		enqueue(readyFrame)
	}

	return new Response(body, {
		headers: {
			"content-type": "text/event-stream; charset=utf-8",
			"cache-control": "no-cache, no-transform",
			"x-accel-buffering": "no",
		},
	})
}
