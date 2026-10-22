import type {
	AnySchema,
	ClientApi,
	ClientId,
	CollectionName,
	Cookie,
	Mutation,
	MutationId,
	MutationOp,
	PatchRemoveOp,
	RelationalQuery,
	RelationalQueryResult,
	RemoteApi,
	RemoteMutationGapError,
	RngApi,
	AnyRelations,
	RuntimeSchemaDefinition,
	SchemaToTupleSchema,
} from "@tanishqkancharla/tandem-core"
import { tag, unreachable } from "@tanishqkancharla/tandem-core"
import {
	collectionIdsEqual,
	executeQueryAsync,
	executeScanWindowAsync,
	InvalidScanWindowError,
} from "@tanishqkancharla/tandem-core/internal"
import type { ScanWindowRecord } from "@tanishqkancharla/tandem-core/internal"
import * as errore from "errore"
import {
	AsyncTupleDatabase,
	AsyncTupleDatabaseClient,
	subscribeQueryAsync,
} from "tuple-database"
import type {
	AsyncTupleRootTransactionApi,
	AsyncTupleStorageApi,
	ReadOnlyAsyncTupleDatabaseClientApi,
	WriteOps,
} from "tuple-database"
import { TandemServerError } from "./TandemServerError.js"
import { TandemServerTransaction } from "./TandemServerTransaction.js"
import type {
	TandemClientTuple,
	TandemTuple,
	TandemServerStorageApi,
} from "./storage/TandemServerStorage.js"

export type TandemServerArgs<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
> = {
	schema: RuntimeSchemaDefinition<Schema>
	relations: Relations
	storage: TandemServerStorageApi<NoInfer<Schema>>
	rng?: RngApi
}

export type TandemServerSubscription<Result> = {
	result: Result
	destroy: () => void
}

export type TandemServerSubscriptionOptions = {
	onError?: (error: Error) => void
}

type SyncedRecordKey<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema> = CollectionName<Schema>,
> = {
	[CurrentCollection in Collection]: {
		collection: CurrentCollection
		id: Schema[CurrentCollection]["id"]
	}
}[Collection]

type SyncClientState<Schema extends AnySchema> = {
	poke?: ClientApi["poke"]
	cookie?: Cookie
	syncedRecordKeys?: SyncedRecordKey<Schema>[]
}

type CommitOptions = {
	operation: "commit" | "push"
}

function scanWindowRecordToKey<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
>(
	record: ScanWindowRecord<Schema, Collection>,
): SyncedRecordKey<Schema, Collection> {
	return { collection: record.collection, id: record.value.id }
}

function containsRecordKey<Schema extends AnySchema>(
	records: readonly SyncedRecordKey<Schema>[],
	target: SyncedRecordKey<Schema>,
) {
	return records.some(
		(record) =>
			record.collection === target.collection &&
			collectionIdsEqual(record.id, target.id),
	)
}

function applyMutationOperation<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
>(
	transaction: TandemServerTransaction<Schema, Relations>,
	operation: MutationOp<Schema>,
) {
	switch (operation.type) {
		case "set":
			transaction.set(operation.collection, operation.value)
			return
		case "remove":
			transaction.remove(operation.collection, operation.id)
			return
		default:
			return unreachable(operation)
	}
}

function tandemStorageToTupleDatabaseStorage<Schema extends AnySchema>(
	storage: TandemServerStorageApi<Schema>,
): AsyncTupleStorageApi {
	return {
		scan: (args) => storage.scan(args),
		commit: (writes) => storage.commit(writes as WriteOps<TandemTuple<Schema>>),
		close: () => storage.close(),
	}
}

export class TandemServer<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
> implements RemoteApi<Schema> {
	private readonly relations: Relations
	private readonly subscriptions = new Set<() => void>()
	private readonly syncClients = new Map<ClientId, SyncClientState<Schema>>()
	private readonly tupleDb: AsyncTupleDatabaseClient<TandemTuple<Schema>>
	private readonly rng?: RngApi
	private readonly unsubscribeStorage?: () => void
	private closed = false
	// Tuple transactions batch writes but do not snapshot reads or serialize
	// asynchronous storage commits. Order sync reads and commits here.
	private queue: Promise<void> = Promise.resolve()

	constructor(args: TandemServerArgs<Schema, Relations>) {
		this.relations = args.relations
		this.rng = args.rng
		const database = new AsyncTupleDatabase(
			tandemStorageToTupleDatabaseStorage(args.storage),
			{ rng: args.rng },
		)
		this.tupleDb = new AsyncTupleDatabaseClient<TandemTuple<Schema>>(database)
		this.unsubscribeStorage = args.storage.subscribe?.(() => {
			if (this.closed) return
			this.emitPokes()
		})
	}

	private get recordDb() {
		// Query APIs retain full ["record", ...] keys. This is the same client,
		// narrowed for those APIs, not a subspace that strips the record prefix.
		return this.tupleDb as unknown as AsyncTupleDatabaseClient<
			SchemaToTupleSchema<Schema>
		>
	}

	transact(): TandemServerTransaction<Schema, Relations> {
		return new TandemServerTransaction(
			this.tupleDb.transact(this.rng?.randomId()),
			this.relations,
		)
	}

	commit(
		transaction: TandemServerTransaction<Schema, Relations>,
	): Promise<void> {
		return this.run(() =>
			this.commitTransaction(transaction, { operation: "commit" }),
		)
	}

	connect = ({ clientId, poke }: ClientApi) => {
		const client = this.getSyncClient(clientId)
		client.poke = poke
		let disconnected = false

		return Promise.resolve(() => {
			if (disconnected) return Promise.resolve()
			disconnected = true

			const current = this.syncClients.get(clientId)
			if (current?.poke === poke) current.poke = undefined
			return Promise.resolve()
		})
	}

	push: RemoteApi<Schema>["push"] = ({ clientId, mutations }) =>
		this.run(async () => {
			const result = await this.applyPush(clientId, mutations)
			if (result instanceof Error) {
				return { error: "unavailable", message: result.message } as const
			}
			return result ?? ({ ok: true } as const)
		})

	pull: RemoteApi<Schema>["pull"] = (args) =>
		this.run(async () => {
			const result = await this.readPull(args)
			if (result instanceof Error) {
				return { error: "unavailable", message: result.message } as const
			}
			return result
		})

	private run<T>(operation: () => Promise<T | TandemServerError>): Promise<T> {
		const result = this.queue.then(async () => {
			const value = await operation()
			if (value instanceof Error) throw value
			return value
		})
		this.queue = result.then(
			() => undefined,
			() => undefined,
		)
		return result
	}

	async query<Query extends RelationalQuery<Schema, Relations>>(
		query: Query,
	): Promise<RelationalQueryResult<Schema, Relations, Query>> {
		const result = await this.runQuery(this.recordDb, query, "query")
		if (result instanceof Error) throw result
		return result
	}

	async subscribe<Query extends RelationalQuery<Schema, Relations>>(
		query: Query,
		callback: (result: RelationalQueryResult<Schema, Relations, Query>) => void,
		options: TandemServerSubscriptionOptions = {},
	): Promise<
		TandemServerSubscription<RelationalQueryResult<Schema, Relations, Query>>
	> {
		const subscription = await subscribeQueryAsync(
			this.recordDb,
			(db) => this.runQuery(db, query, "subscribe"),
			(result) => {
				if (result instanceof Error) {
					if (options.onError) options.onError(result)
					else console.error(result)
					return
				}
				callback(result)
			},
		).catch((cause) => new TandemServerError({ operation: "subscribe", cause }))

		if (subscription instanceof Error) throw subscription
		if (subscription.result instanceof Error) {
			subscription.destroy()
			throw subscription.result
		}

		let destroyed = false
		const destroy = () => {
			if (destroyed) return
			destroyed = true
			subscription.destroy()
			this.subscriptions.delete(destroy)
		}
		this.subscriptions.add(destroy)

		return { result: subscription.result, destroy }
	}

	async close(): Promise<void> {
		this.closed = true
		this.unsubscribeStorage?.()
		for (const destroy of this.subscriptions) destroy()
		this.syncClients.clear()

		const result = await this.tupleDb
			.close()
			.catch((cause) => new TandemServerError({ operation: "close", cause }))
		if (result instanceof Error) throw result
	}

	private runQuery<Query extends RelationalQuery<Schema, Relations>>(
		db: ReadOnlyAsyncTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
		query: Query,
		operation: "query" | "subscribe",
	): Promise<
		TandemServerError | RelationalQueryResult<Schema, Relations, Query>
	> {
		return executeQueryAsync<Schema, Relations, Query>(
			db,
			this.relations,
			query,
		).catch((cause) => new TandemServerError({ operation, cause }))
	}

	private getSyncClient(clientId: ClientId): SyncClientState<Schema> {
		const current = this.syncClients.get(clientId)
		if (current) return current

		const created: SyncClientState<Schema> = {}
		this.syncClients.set(clientId, created)
		return created
	}

	private async applyPush(
		clientId: ClientId,
		mutations: Mutation<Schema>[],
	): Promise<TandemServerError | RemoteMutationGapError | undefined> {
		for (const mutation of mutations) {
			const lastMutationId = await this.readLastMutationId(clientId, "push")
			if (lastMutationId instanceof Error) return lastMutationId
			if (mutation.id <= lastMutationId) continue
			if (mutation.id !== lastMutationId + 1) {
				return {
					error: "mutation-gap",
					expectedMutationId: lastMutationId + 1,
					receivedMutationId: mutation.id,
				}
			}

			const tupleTx = this.tupleDb.transact(this.rng?.randomId())
			const transaction = new TandemServerTransaction(tupleTx, this.relations)
			// Narrow before selecting the namespace: tuple-database cannot resolve
			// subspace types through the generic application-record union.
			const clientTx = (
				tupleTx as unknown as AsyncTupleRootTransactionApi<TandemClientTuple>
			).subspace(["client"])
			const staged = errore.try(() => {
				for (const operation of mutation.ops) {
					applyMutationOperation(transaction, operation)
				}
				clientTx.set([clientId], { lastMutationId: mutation.id })
			})
			if (staged instanceof Error) {
				await transaction.cancel()
				return new TandemServerError({ operation: "push", cause: staged })
			}

			const committed = await this.commitTransaction(transaction, {
				operation: "push",
			})
			if (committed instanceof Error) return committed
		}
	}

	private async readLastMutationId(
		clientId: ClientId,
		operation: "push" | "pull",
	) {
		const metadata = (
			this.tupleDb as unknown as AsyncTupleDatabaseClient<TandemClientTuple>
		).subspace(["client"])
		const client = await metadata
			.get([clientId])
			.catch((cause) => new TandemServerError({ operation, cause }))
		if (client instanceof Error) return client
		return client?.lastMutationId ?? tag<MutationId>(0)
	}

	private async readPull(
		args: Parameters<RemoteApi<Schema>["pull"]>[0],
	): Promise<
		TandemServerError | Awaited<ReturnType<RemoteApi<Schema>["pull"]>>
	> {
		const { clientId, cookie, scanWindow } = args
		const client = this.getSyncClient(clientId)
		const serialized = errore.try(() => JSON.stringify(scanWindow))
		if (serialized instanceof Error)
			return {
				error: "invalid-request",
				message: "Scan window must be serializable",
			}
		const lastMutationId = await this.readLastMutationId(clientId, "pull")
		if (lastMutationId instanceof Error) return lastMutationId

		const shouldReset = cookie === undefined || cookie !== client.cookie
		const records = await executeScanWindowAsync<Schema, Relations>(
			this.recordDb,
			this.relations,
			scanWindow,
		).catch((cause) => new TandemServerError({ operation: "pull", cause }))
		if (records instanceof InvalidScanWindowError) {
			return { error: "invalid-request", message: records.message }
		}
		if (records instanceof Error) return records

		const currentRecordKeys = records.map((record) =>
			scanWindowRecordToKey(record),
		)
		const remove: PatchRemoveOp<Schema>[] = shouldReset
			? []
			: (client.syncedRecordKeys ?? []).filter(
					(previous) => !containsRecordKey(currentRecordKeys, previous),
				)
		const nextCookie = tag<Cookie>(this.rng?.randomId() ?? crypto.randomUUID())
		client.syncedRecordKeys = currentRecordKeys
		client.cookie = nextCookie

		return {
			cookie: nextCookie,
			patch: { ...(shouldReset ? { reset: true } : {}), set: records, remove },
			lastMutationId,
		}
	}

	private async commitTransaction(
		transaction: TandemServerTransaction<Schema, Relations>,
		options: CommitOptions,
	): Promise<TandemServerError | undefined> {
		const hasWrites = await transaction
			.commit()
			.catch(
				(cause) =>
					new TandemServerError({ operation: options.operation, cause }),
			)
		if (hasWrites instanceof Error) return hasWrites
		if (!hasWrites) return

		this.emitPokes()
	}

	private emitPokes() {
		for (const client of this.syncClients.values()) {
			if (!client.poke) continue

			const result = errore.try(client.poke)
			if (result instanceof Error) {
				console.error(
					new TandemServerError({ operation: "poke", cause: result }),
				)
			}
		}
	}
}
