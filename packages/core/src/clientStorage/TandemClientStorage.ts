import type { KeyValuePair, ScanStorageArgs, WriteOps } from "tuple-database"
import type {
	AnySchema,
	CollectionIdTuple,
	CollectionName,
	SchemaToTupleSchema,
} from "../schema/Schema.js"
import type { ScanWindow } from "../query/Query.js"
import type { ClientId, Cookie } from "../sync/SyncEngine.js"
import type { Mutation, MutationId } from "../transaction/Transaction.js"
import { isEqual } from "../utils/objectUtils.js"

export type TandemClientBase<Record> = {
	server: { type: "set"; value: Record } | { type: "remove" }
	lastWrittenBy: MutationId
}

/** Durable replica tuples. Sync hydration and delivery are owned by SyncEngine. */
export type TandemClientTuple<Schema extends AnySchema> =
	| SchemaToTupleSchema<Schema>
	| {
			[Collection in CollectionName<Schema>]: {
				key: [
					"base",
					Collection,
					...CollectionIdTuple<Schema[Collection]["id"]>,
				]
				value: TandemClientBase<Schema[Collection]>
			}
	  }[CollectionName<Schema>]
	| {
			key: ["replica"]
			value: {
				clientId: ClientId
				lastIssuedMutationId: MutationId
				cookie?: Cookie
			}
	  }
	| {
			key: ["outbox", MutationId]
			value: Mutation<Schema>["ops"]
	  }
	| {
			/** A tab session's last forwarded mutation and the queries it syncs. */
			key: ["session", ClientId]
			value: {
				lastMutationId?: MutationId
				scanWindow?: ScanWindow<Schema>
			}
	  }

export interface TandemClientStorageApi<Schema extends AnySchema> {
	/**
	 * Identifies storage that several tabs can open at once. Clients with the
	 * same name elect one tab to own the storage and sync for the others.
	 */
	readonly name?: string
	scan(args?: ScanStorageArgs): Promise<TandemClientTuple<Schema>[]>
	/** Apply the whole batch atomically; resolve only after it is durable. */
	commit(writes: WriteOps<TandemClientTuple<Schema>>): Promise<void>
	close(): Promise<void>
	clear(): Promise<void>
}

export namespace WriteOpsApi {
	export function toString<TupleSchema extends KeyValuePair>(
		writeOps: WriteOps<TupleSchema>,
	): string {
		return `WriteOps {\n${
			writeOps.set
				?.map(
					(op) =>
						`  set (${op.key[1]}) ${JSON.stringify(op.value, undefined, 2)}`,
				)
				.join("\n") ?? ""
		}\n${writeOps.remove?.map((op) => `  remove (${op})`).join("\n") ?? ""}}`
	}

	export function merge<TupleSchema extends KeyValuePair>(
		...allWriteOps: WriteOps<TupleSchema>[]
	): WriteOps<TupleSchema> {
		const target: WriteOps<TupleSchema> = {
			set: [],
			remove: [],
		}

		for (const { set = [], remove = [] } of allWriteOps) {
			for (const tuple of set) {
				const { key } = tuple
				// Filter out all the keys that we marked as set
				target.remove = target.remove?.filter(
					(removedKey) => !isEqual(removedKey, key),
				)

				target.set ??= []
				target.set.push(tuple)
			}

			for (const key of remove) {
				target.set = target.set?.filter(
					({ key: keySet }) => !isEqual(keySet, key),
				)

				target.remove ??= []
				target.remove.push(key)
			}
		}

		return target
	}
}

export class TandemClientStorage<Schema extends AnySchema> {
	constructor(
		readonly adapter: TandemClientStorageApi<Schema>,
		private readonly onFailure: (error: unknown) => void,
	) {}

	async commit(writeOps: WriteOps<TandemClientTuple<Schema>>) {
		try {
			await this.adapter.commit(writeOps)
		} catch (error) {
			this.onFailure(error)
			throw error
		}
	}

	async scan(args?: ScanStorageArgs): Promise<TandemClientTuple<Schema>[]> {
		// TODO: what should we do if this fails?
		return await this.adapter.scan(args)
	}

	async clear() {
		await this.adapter.clear()
	}
}
