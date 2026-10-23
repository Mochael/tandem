import * as errore from "errore"

export type TandemServerOperation =
	| "cancel"
	| "close"
	| "commit"
	| "connect"
	| "disconnect"
	| "poke"
	| "pull"
	| "push"
	| "get"
	| "list"
	| "query"
	| "remove"
	| "scan"
	| "set"
	| "subscribe"
	| "update"

export class TandemServerError extends errore.createTaggedError({
	name: "TandemServerError",
	message: "Tandem server $operation failed",
}) {}
