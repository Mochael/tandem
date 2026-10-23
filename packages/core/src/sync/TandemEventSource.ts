import type { AsyncUnsubscribe } from "../utils/typeUtils.js"
import type { ClientApi } from "./SyncEngine.js"

export type TandemEventSourceArgs = {
	/** An event stream such as one served by `createTandemEventStreamResponse`. */
	url: string | URL
	poke: ClientApi["poke"]
	/** Milliseconds between fallback pulls, or `null` to disable them. Defaults to 60 seconds. */
	pullInterval?: number | null
	/** Milliseconds before reopening a stream the browser stopped retrying. Defaults to 5 seconds. */
	reconnectDelay?: number
	eventSourceInit?: EventSourceInit
}

/**
 * Pokes the client from server-sent `ready` and `poke` events.
 *
 * `ready` arrives after every (re)connection, so the client pulls changes it
 * missed while disconnected. Fallback pulls bound staleness when events are lost.
 * Returns a function that closes the stream and stops all timers.
 */
export function connectTandemEventSource({
	url,
	poke,
	pullInterval = 60_000,
	reconnectDelay = 5_000,
	eventSourceInit,
}: TandemEventSourceArgs): AsyncUnsubscribe {
	let source: EventSource | undefined
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined
	const onPoke = () => void poke()
	const onError = () => {
		// The browser retries dropped streams itself, but not failed responses.
		if (source?.readyState !== EventSource.CLOSED) return
		close()
		reconnectTimer = setTimeout(open, reconnectDelay)
	}
	const open = () => {
		reconnectTimer = undefined
		source = new EventSource(url, eventSourceInit)
		source.addEventListener("ready", onPoke)
		source.addEventListener("poke", onPoke)
		source.addEventListener("error", onError)
	}
	const close = () => {
		source?.removeEventListener("ready", onPoke)
		source?.removeEventListener("poke", onPoke)
		source?.removeEventListener("error", onError)
		source?.close()
		source = undefined
	}

	open()
	const pullTimer =
		pullInterval === null ? undefined : setInterval(onPoke, pullInterval)

	return () => {
		close()
		clearTimeout(reconnectTimer)
		clearInterval(pullTimer)
		return Promise.resolve()
	}
}
