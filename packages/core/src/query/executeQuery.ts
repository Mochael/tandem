import * as errore from "errore"
import type {
	ReadOnlyAsyncTupleDatabaseClientApi,
	ReadOnlyTupleDatabaseClientApi,
} from "tuple-database"
import type {
	AnySchema,
	CollectionName,
	AnyRelations,
	SchemaToTupleSchema,
} from "../schema/Schema.js"
import { collectionIdToTuple } from "../schema/Schema.js"
import { isEqual, isObject, pick } from "../utils/objectUtils.js"
import { match } from "../utils/typeUtils.js"
import type {
	EncodedQuery,
	Operator,
	RelationalQuery,
	RelationalQueryResult,
	ScanWindow,
} from "./Query.js"

type RuntimeRecord = Record<string, unknown>

export class InvalidScanWindowError extends errore.createTaggedError({
	name: "InvalidScanWindowError",
	message: "$detail",
}) {}

type RelationalQueryInput<Schema extends AnySchema> = {
	readonly collection: CollectionName<Schema>
	readonly select?: Readonly<Record<string, true | undefined>>
	readonly where?: Readonly<Record<string, unknown>>
	readonly with?: Readonly<
		Record<
			string,
			true | Omit<RelationalQueryInput<Schema>, "collection"> | undefined
		>
	>
	readonly orderBy?: Readonly<Record<string, "asc" | "desc" | undefined>>
	readonly limit?: number
	readonly offset?: number
}

type QueryWhereClause = readonly [
	field: string,
	operator: Operator,
	value: unknown,
]

type QueryOrderClause = readonly [field: string, direction: "asc" | "desc"]

type QueryNode<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema> = CollectionName<Schema>,
> = {
	readonly collection: Collection
	readonly select?: readonly string[]
	readonly where?: readonly QueryWhereClause[]
	readonly with?: Readonly<Record<string, QueryNode<Schema>>>
	readonly order?: readonly QueryOrderClause[]
	readonly limit?: number
	readonly offset?: number
}

type RecordsByCollection<Schema extends AnySchema> = {
	readonly [
		Collection in CollectionName<Schema>
	]?: readonly Schema[Collection][]
}

type MutableRecordsByCollection<Schema extends AnySchema> = {
	-readonly [Collection in CollectionName<Schema>]?: Schema[Collection][]
}

type QueryRowCollector<Schema extends AnySchema> = <
	Collection extends CollectionName<Schema>,
>(
	collection: Collection,
	record: Schema[Collection],
) => void

export type ScanWindowRecord<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema> = CollectionName<Schema>,
> = {
	[CurrentCollection in Collection]: {
		collection: CurrentCollection
		value: Schema[CurrentCollection]
	}
}[Collection]

function isRelationalWhereOperator(operator: string) {
	return (
		operator === "eq" ||
		operator === "gt" ||
		operator === "lt" ||
		operator === "gte" ||
		operator === "lte"
	)
}

function encodeRelationalWhereOperator(operator: string): Operator {
	if (operator === "eq") return "="
	if (operator === "gt") return ">"
	if (operator === "lt") return "<"
	if (operator === "gte") return ">="
	if (operator === "lte") return "<="
	throw new Error(`Unknown where operator "${operator}"`)
}

function normalizeRelationalQuery<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
	Query extends RelationalQuery<Schema, Relations>,
>(query: Query, relations: Relations | undefined): QueryNode<Schema>
function normalizeRelationalQuery<Schema extends AnySchema>(
	query: RelationalQueryInput<Schema>,
	relations: AnyRelations<Schema> | undefined,
): QueryNode<Schema>
function normalizeRelationalQuery<Schema extends AnySchema>(
	query: RelationalQueryInput<Schema>,
	relations: AnyRelations<Schema> | undefined,
): QueryNode<Schema> {
	const select: string[] = []
	for (const field in query.select) {
		if (query.select[field]) select.push(field)
	}

	const where: QueryWhereClause[] = []
	for (const field in query.where) {
		const condition = query.where[field]
		if (!isObject(condition) || Array.isArray(condition)) {
			where.push([field, "=", condition])
			continue
		}

		for (const operator in condition) {
			if (!isRelationalWhereOperator(operator)) {
				throw new Error(`Unknown where operator "${operator}"`)
			}
			where.push([
				field,
				encodeRelationalWhereOperator(operator),
				condition[operator],
			])
		}
	}

	const order: QueryOrderClause[] = []
	for (const field in query.orderBy) {
		const direction = query.orderBy[field]
		if (direction) order.push([field, direction])
	}

	const normalized = {
		collection: query.collection,
		...(query.select ? { select } : {}),
		...(query.where ? { where } : {}),
		...(query.orderBy ? { order } : {}),
		...(query.limit === undefined ? {} : { limit: query.limit }),
		...(query.offset === undefined ? {} : { offset: query.offset }),
	}
	if (!query.with) return normalized
	if (!relations) {
		throw new Error(
			"Cannot execute relational query includes without relations",
		)
	}

	const withQueries: Record<string, QueryNode<Schema>> = {}
	for (const relationName in query.with) {
		const includeOptions = query.with[relationName]
		if (!includeOptions) continue
		const relation = relations[query.collection]?.[relationName]
		if (!relation) {
			throw new Error(`Unknown relation "${query.collection}.${relationName}"`)
		}

		withQueries[relationName] = normalizeRelationalQuery(
			{
				collection: relation.targetCollection,
				...(includeOptions === true ? {} : includeOptions),
			},
			relations,
		)
	}

	return { ...normalized, with: withQueries }
}

function normalizeEncodedQuery<Schema extends AnySchema>(
	query: EncodedQuery<Schema>,
	relations: AnyRelations<Schema>,
): QueryNode<Schema> | InvalidScanWindowError {
	const normalized = {
		collection: query.collection,
		...(query.select === undefined || query.select === "*"
			? {}
			: { select: query.select }),
		...(query.where?.length ? { where: query.where } : {}),
		...(query.order?.length ? { order: query.order } : {}),
		...(query.limit === undefined ? {} : { limit: query.limit }),
		...(query.offset === undefined ? {} : { offset: query.offset }),
	}
	if (!query.with) return normalized

	const withQueries: Record<string, QueryNode<Schema>> = {}
	for (const relationName in query.with) {
		const nestedQuery = query.with[relationName]
		const relation = relations[query.collection]?.[relationName]
		if (!relation) {
			return new InvalidScanWindowError({
				detail: `Unknown relation "${query.collection}.${relationName}"`,
			})
		}
		if (nestedQuery.collection !== relation.targetCollection) {
			return new InvalidScanWindowError({
				detail: `Relation "${query.collection}.${relationName}" targets collection "${relation.targetCollection}", not "${nestedQuery.collection}"`,
			})
		}

		const normalizedNested = normalizeEncodedQuery(nestedQuery, relations)
		if (normalizedNested instanceof Error) return normalizedNested
		withQueries[relationName] = normalizedNested
	}

	return { ...normalized, with: withQueries }
}

function collectQueryCollections<Schema extends AnySchema>(
	query: QueryNode<Schema>,
	collections: Set<CollectionName<Schema>>,
) {
	collections.add(query.collection)
	if (!query.with) return

	for (const nestedQuery of Object.values(query.with)) {
		collectQueryCollections(nestedQuery, collections)
	}
}

function getQueryCollections<Schema extends AnySchema>(
	queries: readonly QueryNode<Schema>[],
) {
	const collections = new Set<CollectionName<Schema>>()
	for (const query of queries) collectQueryCollections(query, collections)
	return collections
}

function scanCollectionSync<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
>(
	db: ReadOnlyTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	collection: Collection,
): Schema[Collection][]
function scanCollectionSync<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
>(
	db: ReadOnlyTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	collection: Collection,
): RuntimeRecord[] {
	const collectionDb = db as unknown as {
		scan(args: { prefix: ["record", Collection] }): {
			value: Schema[Collection]
		}[]
	}
	return collectionDb
		.scan({ prefix: ["record", collection] })
		.map(({ value }) => value)
}

async function scanCollectionAsync<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
>(
	db: ReadOnlyAsyncTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	collection: Collection,
): Promise<Schema[Collection][]>
async function scanCollectionAsync<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
>(
	db: ReadOnlyAsyncTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	collection: Collection,
): Promise<RuntimeRecord[]> {
	const collectionDb = db as unknown as {
		scan(args: {
			prefix: ["record", Collection]
		}): Promise<{ value: Schema[Collection] }[]>
	}
	const tuples = await collectionDb.scan({ prefix: ["record", collection] })
	return tuples.map(({ value }) => value)
}

function getCollectionRecords<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
>(
	recordsByCollection: RecordsByCollection<Schema>,
	collection: Collection,
): readonly Schema[Collection][]
function getCollectionRecords<Schema extends AnySchema>(
	recordsByCollection: RecordsByCollection<Schema>,
	collection: CollectionName<Schema>,
): readonly RuntimeRecord[]
function getCollectionRecords<Schema extends AnySchema>(
	recordsByCollection: RecordsByCollection<Schema>,
	collection: CollectionName<Schema>,
): readonly RuntimeRecord[] {
	return recordsByCollection[collection] ?? []
}

function loadRecordsSync<Schema extends AnySchema>(
	db: ReadOnlyTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	collections: ReadonlySet<CollectionName<Schema>>,
): RecordsByCollection<Schema> {
	const records: MutableRecordsByCollection<Schema> = {}
	for (const collection of collections) {
		records[collection] = scanCollectionSync(db, collection)
	}
	return records
}

async function loadRecordsAsync<Schema extends AnySchema>(
	db: ReadOnlyAsyncTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	collections: ReadonlySet<CollectionName<Schema>>,
): Promise<RecordsByCollection<Schema>> {
	const records: MutableRecordsByCollection<Schema> = {}
	for (const collection of collections) {
		records[collection] = await scanCollectionAsync(db, collection)
	}
	return records
}

function compareOrderedValues(left: unknown, right: unknown) {
	if (typeof left === "number" && typeof right === "number") {
		if (left > right) return 1
		if (left < right) return -1
		return 0
	}

	if (typeof left !== "string" || typeof right !== "string") return 0
	if (left > right) return 1
	if (left < right) return -1
	return 0
}

function compareQueryValue(
	fieldValue: unknown,
	operator: Operator,
	comparisonValue: unknown,
) {
	if (operator === "=") return isEqual(fieldValue, comparisonValue)

	const comparison = compareOrderedValues(fieldValue, comparisonValue)
	if (operator === ">") return comparison > 0
	if (operator === "<") return comparison < 0
	if (operator === ">=") return comparison >= 0
	return comparison <= 0
}

function matchesWhere(record: RuntimeRecord, clause: QueryWhereClause) {
	const [field, operator, comparisonValue] = clause
	return compareQueryValue(record[field], operator, comparisonValue)
}

function getQueryRows<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
>(
	query: QueryNode<Schema, Collection>,
	recordsByCollection: RecordsByCollection<Schema>,
	extraFilter?: (record: Schema[Collection]) => boolean,
): Schema[Collection][] {
	const collectionRecords = getCollectionRecords(
		recordsByCollection,
		query.collection,
	)
	const relationFiltered = extraFilter
		? collectionRecords.filter(extraFilter)
		: [...collectionRecords]
	const whereFiltered = query.where
		? relationFiltered.filter((record) =>
				query.where?.every((clause) => matchesWhere(record, clause)),
			)
		: relationFiltered
	const ordered = query.order
		? [...whereFiltered].sort((left, right) => {
				for (const [field, direction] of query.order ?? []) {
					const comparison = compareOrderedValues(left[field], right[field])
					if (comparison !== 0) {
						return match(direction, { asc: comparison, desc: -comparison })
					}
				}
				return 0
			})
		: whereFiltered
	const offset =
		query.offset === undefined ? ordered : ordered.slice(query.offset)

	return query.limit === undefined ? offset : offset.slice(0, query.limit)
}

function executeLoadedQuery<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
	Query extends RelationalQuery<Schema, Relations>,
>(
	query: QueryNode<Schema>,
	relations: Relations | undefined,
	recordsByCollection: RecordsByCollection<Schema>,
	extraFilter?: (record: RuntimeRecord) => boolean,
	collectRow?: QueryRowCollector<Schema>,
): RelationalQueryResult<Schema, Relations, Query>
function executeLoadedQuery<Schema extends AnySchema>(
	query: QueryNode<Schema>,
	relations: AnyRelations<Schema> | undefined,
	recordsByCollection: RecordsByCollection<Schema>,
	extraFilter?: (record: RuntimeRecord) => boolean,
	collectRow?: QueryRowCollector<Schema>,
): RuntimeRecord[]
function executeLoadedQuery<Schema extends AnySchema>(
	query: QueryNode<Schema>,
	relations: AnyRelations<Schema> | undefined,
	recordsByCollection: RecordsByCollection<Schema>,
	extraFilter?: (record: RuntimeRecord) => boolean,
	collectRow?: QueryRowCollector<Schema>,
): RuntimeRecord[] {
	const rows = getQueryRows(query, recordsByCollection, extraFilter)

	return rows.map((row) => {
		collectRow?.(query.collection, row)
		const runtimeRow: RuntimeRecord = row
		const result: RuntimeRecord = query.select
			? pick(runtimeRow, query.select)
			: { ...runtimeRow }
		if (!query.with) return result
		if (!relations) {
			throw new Error(
				"Cannot execute relational query includes without relations",
			)
		}

		for (const [relationName, nestedQuery] of Object.entries(query.with)) {
			const relation = relations[query.collection]?.[relationName]
			if (!relation) {
				throw new Error(
					`Unknown relation "${query.collection}.${relationName}"`,
				)
			}

			const joinValue = runtimeRow[relation.from]
			const related = executeLoadedQuery(
				nestedQuery,
				relations,
				recordsByCollection,
				(target) => target[relation.to] === joinValue,
				collectRow,
			)

			result[relationName] = match<unknown>(relation.type, {
				"many-to-one": related[0] ?? null,
				"one-to-many": related,
			})
		}

		return result
	})
}

function createScanWindowRecord<
	Schema extends AnySchema,
	Collection extends CollectionName<Schema>,
>(
	collection: Collection,
	value: Schema[Collection],
): ScanWindowRecord<Schema, Collection> {
	return { collection, value }
}

export function executeQuerySync<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
	Query extends RelationalQuery<Schema, Relations>,
>(
	db: ReadOnlyTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	relations: Relations | undefined,
	query: Query,
): RelationalQueryResult<Schema, Relations, Query> {
	const normalizedQuery = normalizeRelationalQuery(query, relations)
	const collections = getQueryCollections([normalizedQuery])
	const records = loadRecordsSync(db, collections)

	return executeLoadedQuery<Schema, Relations, Query>(
		normalizedQuery,
		relations,
		records,
	)
}

export async function executeQueryAsync<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
	Query extends RelationalQuery<Schema, Relations>,
>(
	db: ReadOnlyAsyncTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	relations: Relations | undefined,
	query: Query,
): Promise<RelationalQueryResult<Schema, Relations, Query>> {
	const normalizedQuery = normalizeRelationalQuery(query, relations)
	const collections = getQueryCollections([normalizedQuery])
	const records = await loadRecordsAsync(db, collections)

	return executeLoadedQuery<Schema, Relations, Query>(
		normalizedQuery,
		relations,
		records,
	)
}

function normalizeScanWindow<Schema extends AnySchema>(
	scanWindow: ScanWindow<Schema>,
	relations: AnyRelations<Schema>,
): QueryNode<Schema>[] | InvalidScanWindowError {
	const queries: QueryNode<Schema>[] = []
	for (const query of scanWindow) {
		const normalized = normalizeEncodedQuery(query, relations)
		if (normalized instanceof Error) return normalized
		queries.push(normalized)
	}
	return queries
}

function collectScanWindow<Schema extends AnySchema>(
	queries: readonly QueryNode<Schema>[],
	relations: AnyRelations<Schema>,
	recordsByCollection: RecordsByCollection<Schema>,
): ScanWindowRecord<Schema>[] {
	const result: ScanWindowRecord<Schema>[] = []
	const seen = new Map<CollectionName<Schema>, Set<string>>()
	const collectRow: QueryRowCollector<Schema> = (collection, value) => {
		const collectionIds = seen.get(collection) ?? new Set<string>()
		const idKey = JSON.stringify(collectionIdToTuple(value.id))
		if (collectionIds.has(idKey)) return

		collectionIds.add(idKey)
		seen.set(collection, collectionIds)
		result.push(createScanWindowRecord(collection, value))
	}

	for (const query of queries) {
		executeLoadedQuery(
			query,
			relations,
			recordsByCollection,
			undefined,
			collectRow,
		)
	}

	return result
}

export async function executeScanWindowAsync<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
>(
	db: ReadOnlyAsyncTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	relations: Relations,
	scanWindow: ScanWindow<Schema>,
): Promise<ScanWindowRecord<Schema>[] | InvalidScanWindowError> {
	const queries = normalizeScanWindow(scanWindow, relations)
	if (queries instanceof Error) return queries
	const recordsByCollection = await loadRecordsAsync(
		db,
		getQueryCollections(queries),
	)
	return collectScanWindow(queries, relations, recordsByCollection)
}

export function executeScanWindowSync<
	Schema extends AnySchema,
	Relations extends AnyRelations<Schema>,
>(
	db: ReadOnlyTupleDatabaseClientApi<SchemaToTupleSchema<Schema>>,
	relations: Relations,
	scanWindow: ScanWindow<Schema>,
): ScanWindowRecord<Schema>[] | InvalidScanWindowError {
	const queries = normalizeScanWindow(scanWindow, relations)
	if (queries instanceof Error) return queries
	const recordsByCollection = loadRecordsSync(db, getQueryCollections(queries))
	return collectScanWindow(queries, relations, recordsByCollection)
}
