import type {
	TupleRootTransactionApi,
	TupleTransactionApi,
} from "tuple-database"
import type {
	TandemClientBase,
	TandemClientTuple,
} from "../clientStorage/TandemClientStorage.js"
import type {
	AnySchema,
	CollectionIdPart,
	CollectionIdTuple,
	CollectionName,
	CollectionScanArgs,
} from "../schema/Schema.js"
import { collectionIdToTuple } from "../schema/Schema.js"
import type { EncodedQuery, ScanWindow } from "../query/Query.js"
import type { Tagged } from "../utils/typeUtils.js"

// Namespace-wide reconciliation handles records from any collection in Schema.
// Public collection methods below retain collection-specific record and ID types.
export type TupleClientRecord<Schema extends AnySchema> = {
	key: [collection: CollectionName<Schema>, ...id: CollectionIdPart[]]
	value: Schema[CollectionName<Schema>]
}

export type TupleClientBase<Schema extends AnySchema> = {
	key: TupleClientRecord<Schema>["key"]
	value: TandemClientBase<Schema[CollectionName<Schema>]>
}

export type TupleClientOutbox<Schema extends AnySchema> = {
	key: [MutationId]
	value: MutationOp<Schema>[]
}

type CollectionTupleKey<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
> = CollectionIdTuple<Schema[Collection]["id"]>

type CollectionTransactionApi<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
> = {
	scan(args?: CollectionScanArgs<Schema[Collection]["id"]>): {
		key: CollectionTupleKey<Schema, Collection>
		value: Schema[Collection]
	}[]
	get(
		key: CollectionTupleKey<Schema, Collection>,
	): Schema[Collection] | undefined
	set(
		key: CollectionTupleKey<Schema, Collection>,
		value: Schema[Collection],
	): unknown
	remove(key: CollectionTupleKey<Schema, Collection>): unknown
}

export function getCollectionTransaction<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
>(
	transaction: TupleRootTransactionApi<TandemClientTuple<AnySchema>>,
	collection: Collection,
): CollectionTransactionApi<Schema, Collection> {
	// tuple-database's key filtering cannot reduce a mapped tuple union while
	// Schema is generic. Narrow the transaction once per selected collection.
	const collectionRoot = transaction as unknown as {
		subspace(
			prefix: ["record", Collection],
		): CollectionTransactionApi<Schema, Collection>
	}
	return collectionRoot.subspace(["record", collection])
}

export type SetMutationOp<Schema extends AnySchema> = {
	type: "set"
} & {
	[Collection in CollectionName<Schema>]: {
		collection: Collection
		value: Schema[Collection]
	}
}[CollectionName<Schema>]

export type RemoveMutationOp<Schema extends AnySchema> = {
	type: "remove"
} & {
	[Collection in CollectionName<Schema>]: {
		collection: Collection
		id: Schema[Collection]["id"]
	}
}[CollectionName<Schema>]

export type MutationOp<Schema extends AnySchema> =
	| SetMutationOp<Schema>
	| RemoveMutationOp<Schema>

/**
 * A per-client counter: a client's first committed mutation is 1, and each
 * commit adds 1. A server acknowledges every mutation up to an id at once.
 */
export type MutationId = Tagged<"MutationId", number>
export type Mutation<Schema extends AnySchema> = {
	ops: MutationOp<Schema>[]
	id: MutationId
}

export namespace MutationApi {
	function opToDebugString(op: MutationOp<AnySchema>): string {
		switch (op.type) {
			case "set":
				return `set (${op.collection}) ${JSON.stringify(
					op.value,
					undefined,
					2,
				)}`
			case "remove":
				return `remove (${op.collection}) ${op.id}`
		}
	}

	export function toString<Schema extends AnySchema>(
		mutation: Mutation<Schema>,
	): string {
		return `Mutation {\n${mutation.ops
			.map(opToDebugString)
			.map((s) => `  ${s}`)
			.join("\n")}\n}`
	}

	export function intersectsQuery<Schema extends AnySchema>(
		mutation: Mutation<Schema>,
		query: EncodedQuery<Schema>,
	): boolean {
		for (const op of mutation.ops) {
			const { collection } = query
			if (op.collection !== collection) continue

			// TODO: use select and where
			return true
		}

		return Object.values(query.with ?? {}).some((includedQuery) =>
			intersectsQuery(mutation, includedQuery),
		)
	}

	export function intersectsScanWindow<Schema extends AnySchema>(
		mutation: Mutation<Schema>,
		scanWindow: ScanWindow<Schema>,
	): boolean {
		return scanWindow.some((query) => intersectsQuery(mutation, query))
	}
}

export class Transaction<Schema extends AnySchema> {
	/**
	 * @internal
	 */
	readonly ops: MutationOp<Schema>[] = []

	/** @internal */
	readonly tupleDbTx: TupleRootTransactionApi<TandemClientTuple<AnySchema>>

	constructor(
		tupleDbTx: TupleRootTransactionApi<TandemClientTuple<AnySchema>>,
	) {
		this.tupleDbTx = tupleDbTx
	}

	// tuple-database cannot reduce our generic mapped schema by prefix. Each
	// subspace fixes the namespace; these assertions restore Schema to its values.
	// These are views of tupleDbTx, not independent transactions.
	/** @internal */
	get records() {
		return this.tupleDbTx.subspace([
			"record",
		]) as unknown as TupleTransactionApi<TupleClientRecord<Schema>>
	}

	/** @internal */
	get bases() {
		return this.tupleDbTx.subspace(["base"]) as unknown as TupleTransactionApi<
			TupleClientBase<Schema>
		>
	}

	/** @internal */
	get outbox() {
		return this.tupleDbTx.subspace([
			"outbox",
		]) as unknown as TupleTransactionApi<TupleClientOutbox<Schema>>
	}

	list<Collection extends CollectionName<Schema>>(
		collection: Collection,
	): Readonly<Schema[Collection]>[] {
		const transaction = getCollectionTransaction<Schema, Collection>(
			this.tupleDbTx,
			collection,
		)
		const results = transaction.scan()

		return results.map((result) => result.value)
	}

	scan<Collection extends CollectionName<Schema>>(
		collection: Collection,
		args?: CollectionScanArgs<Schema[Collection]["id"]>,
	): Readonly<Schema[Collection]>[] {
		const transaction = getCollectionTransaction<Schema, Collection>(
			this.tupleDbTx,
			collection,
		)
		const results = transaction.scan(args)

		return results.map((result) => result.value)
	}

	get<Collection extends CollectionName<Schema>>(
		collection: Collection,
		id: Schema[Collection]["id"],
	): Readonly<Schema[Collection]> | undefined {
		const tupleSchemaKey = collectionIdToTuple(id) as CollectionTupleKey<
			Schema,
			Collection
		>
		return getCollectionTransaction<Schema, Collection>(
			this.tupleDbTx,
			collection,
		).get(tupleSchemaKey)
	}

	set<Collection extends CollectionName<Schema>>(
		collection: Collection,
		record: Schema[Collection],
	): Transaction<Schema> {
		const tupleSchemaKey = collectionIdToTuple(record.id) as CollectionTupleKey<
			Schema,
			Collection
		>
		getCollectionTransaction<Schema, Collection>(
			this.tupleDbTx,
			collection,
		).set(tupleSchemaKey, record)
		this.ops.push({ type: "set", collection, value: record })

		return this
	}

	/**
	 * Updates a record in the database with the given updater function *only if
	 * the record exists*.
	 */
	update<Collection extends CollectionName<Schema>>(
		collection: Collection,
		id: Schema[Collection]["id"],
		updateFn: (record: Readonly<Schema[Collection]>) => Schema[Collection],
	): Transaction<Schema> {
		const tupleSchemaKey = collectionIdToTuple(id) as CollectionTupleKey<
			Schema,
			Collection
		>

		const transaction = getCollectionTransaction<Schema, Collection>(
			this.tupleDbTx,
			collection,
		)
		const prevRecord = transaction.get(tupleSchemaKey)
		if (prevRecord === undefined) return this

		const updatedRecord = updateFn(prevRecord)
		if (updatedRecord === prevRecord) {
			return this
		}

		transaction.set(tupleSchemaKey, updatedRecord)
		this.ops.push({ type: "set", collection, value: updatedRecord })

		return this
	}

	remove<Collection extends CollectionName<Schema>>(
		collection: Collection,
		id: Schema[Collection]["id"],
	): Transaction<Schema> {
		const tupleSchemaKey = collectionIdToTuple(id) as CollectionTupleKey<
			Schema,
			Collection
		>

		getCollectionTransaction<Schema, Collection>(
			this.tupleDbTx,
			collection,
		).remove(tupleSchemaKey)
		// Recorded even when the record isn't local, so the server removes it too.
		this.ops.push({ type: "remove", collection, id })

		return this
	}

	cancel() {
		this.tupleDbTx.cancel()
	}
}
