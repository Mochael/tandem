import type { ScanWindow } from "../query/Query.js"
import type {
	AnySchema,
	CollectionName,
	RuntimeSchemaDefinition,
} from "../schema/Schema.js"
import type {
	ClientId,
	Cookie,
	PatchSetOp,
	PullResponse,
} from "../sync/SyncEngine.js"
import type { Mutation, MutationOp } from "../transaction/Transaction.js"
import type { Codec } from "../utils/Codec.js"
import { unreachable } from "../utils/typeUtils.js"

/** Calls a tab session makes on the leader tab. */
export type TabRequest<Schema extends AnySchema> =
	| { method: "push"; mutations: Mutation<Schema>[] }
	| { method: "pull"; cookie?: Cookie; scanWindow: ScanWindow<Schema> }
	| {
			method:
				| "connect"
				| "disconnect"
				| "pullFromRemote"
				| "flush"
				| "clear"
				| "leave"
	  }

export type TabMessage =
	/** A session asks the current leader to announce itself. */
	| { type: "hello" }
	| { type: "leader"; leaderId: string }
	/** The leader's records changed; sessions should pull. */
	| { type: "poke"; leaderId: string }
	| {
			type: "request"
			leaderId: string
			sessionId: ClientId
			requestId: string
			request: TabRequest<AnySchema>
	  }
	| {
			type: "response"
			leaderId: string
			sessionId: ClientId
			requestId: string
			result: unknown
	  }

// Records cross tabs by structured clone, which drops class instances. Collection
// codecs turn values into plain data first, as IndexedDB storage does.
function getCodec<Schema extends AnySchema>(
	schema: RuntimeSchemaDefinition<Schema> | undefined,
	collection: CollectionName<Schema>,
) {
	return schema?.collections[collection]?.codec as
		| Codec<unknown, unknown>
		| undefined
}

function transformValue<Schema extends AnySchema>(
	schema: RuntimeSchemaDefinition<Schema> | undefined,
	collection: CollectionName<Schema>,
	value: unknown,
	direction: "encode" | "decode",
) {
	const codec = getCodec(schema, collection)
	return codec ? codec[direction](value) : value
}

function transformMutations<Schema extends AnySchema>(
	schema: RuntimeSchemaDefinition<Schema> | undefined,
	mutations: Mutation<Schema>[],
	direction: "encode" | "decode",
): Mutation<Schema>[] {
	return mutations.map(({ id, ops }) => ({
		id,
		ops: ops.map((op): MutationOp<Schema> => {
			switch (op.type) {
				case "set":
					return {
						...op,
						value: transformValue(schema, op.collection, op.value, direction),
					} as MutationOp<Schema>
				case "remove":
					return op
				default:
					return unreachable(op)
			}
		}),
	}))
}

function transformPull<Schema extends AnySchema>(
	schema: RuntimeSchemaDefinition<Schema> | undefined,
	response: PullResponse<Schema>,
	direction: "encode" | "decode",
): PullResponse<Schema> {
	return {
		...response,
		patch: {
			...response.patch,
			set: response.patch.set?.map(
				(op) =>
					({
						collection: op.collection,
						value: transformValue(schema, op.collection, op.value, direction),
					}) as PatchSetOp<Schema>,
			),
		},
	}
}

export namespace TabCodec {
	export const encodeMutations = <Schema extends AnySchema>(
		schema: RuntimeSchemaDefinition<Schema> | undefined,
		mutations: Mutation<Schema>[],
	) => transformMutations(schema, mutations, "encode")

	export const decodeMutations = <Schema extends AnySchema>(
		schema: RuntimeSchemaDefinition<Schema> | undefined,
		mutations: Mutation<Schema>[],
	) => transformMutations(schema, mutations, "decode")

	export const encodePull = <Schema extends AnySchema>(
		schema: RuntimeSchemaDefinition<Schema> | undefined,
		response: PullResponse<Schema>,
	) => transformPull(schema, response, "encode")

	export const decodePull = <Schema extends AnySchema>(
		schema: RuntimeSchemaDefinition<Schema> | undefined,
		response: PullResponse<Schema>,
	) => transformPull(schema, response, "decode")
}
