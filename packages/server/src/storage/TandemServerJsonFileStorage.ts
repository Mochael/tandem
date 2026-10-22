import crypto from "node:crypto"
import fs from "node:fs/promises"
import path from "node:path"
import type { AnySchema } from "@tanishqkancharla/tandem-core"
import * as errore from "errore"
import { InMemoryTupleStorage } from "tuple-database"
import type { ScanStorageArgs, WriteOps } from "tuple-database"
import type {
	TandemTuple,
	TandemServerStorageApi,
} from "./TandemServerStorage.js"

export type TandemServerJsonFileStorageArgs = {
	filePath: string
}

type FileOperation =
	| "clean up temporary file for"
	| "create directory for"
	| "publish"
	| "read"
	| "write"

class TandemServerJsonFileStorageError extends errore.createTaggedError({
	name: "TandemServerJsonFileStorageError",
	message: "Tandem server JSON storage failed to $operation $filePath",
}) {}

function runFileOperation<T>({
	operation,
	filePath,
	promise,
}: {
	operation: FileOperation
	filePath: string
	promise: Promise<T>
}) {
	return promise.catch(
		(cause) =>
			new TandemServerJsonFileStorageError({ operation, filePath, cause }),
	)
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value)
}

function isNotFoundError(value: unknown): boolean {
	return isRecord(value) && value.code === "ENOENT"
}

export class TandemServerJsonFileStorage<
	Schema extends AnySchema = AnySchema,
> implements TandemServerStorageApi<Schema> {
	private queue: Promise<void> = Promise.resolve()

	constructor(private readonly args: TandemServerJsonFileStorageArgs) {}

	scan(args?: ScanStorageArgs): Promise<TandemTuple<Schema>[]> {
		return this.run(async () => {
			const memory = await this.loadFromDisk()
			if (memory instanceof Error) return memory

			// tuple-database's in-memory storage erases its tuple generic. The data
			// comes from typed writes or a file previously written by this adapter.
			return this.scanMemory(memory, args)
		})
	}

	commit(writes: WriteOps<TandemTuple<Schema>>): Promise<void> {
		return this.run(async () => {
			const memory = await this.loadFromDisk()
			if (memory instanceof Error) return memory

			memory.commit(writes)
			return this.writeToDisk(this.scanMemory(memory))
		})
	}

	close(): Promise<void> {
		return this.run(() => undefined)
	}

	private run<T>(
		operation: () =>
			| Promise<T | TandemServerJsonFileStorageError>
			| T
			| TandemServerJsonFileStorageError,
	): Promise<T> {
		const result = this.queue.then(async () => {
			const operationResult = await operation()
			if (operationResult instanceof Error) throw operationResult
			return operationResult
		})
		this.queue = result.then(
			() => undefined,
			() => undefined,
		)
		return result
	}

	private scanMemory(memory: InMemoryTupleStorage, args?: ScanStorageArgs) {
		return memory.scan(args) as TandemTuple<Schema>[]
	}

	private async loadFromDisk() {
		const memory = new InMemoryTupleStorage()
		const raw = await runFileOperation({
			operation: "read",
			filePath: this.args.filePath,
			promise: fs.readFile(this.args.filePath, "utf8"),
		})
		if (raw instanceof Error && isNotFoundError(raw.cause)) return memory
		if (raw instanceof Error) return raw

		const tuples = errore.try({
			try: () => JSON.parse(raw) as TandemTuple<Schema>[],
			catch: (cause) =>
				new TandemServerJsonFileStorageError({
					operation: "parse",
					filePath: this.args.filePath,
					cause,
				}),
		})
		if (tuples instanceof Error) return tuples

		memory.commit({ set: tuples })
		return memory
	}

	private async writeToDisk(tuples: TandemTuple<Schema>[]) {
		const serialized = errore.try({
			try: () => `${JSON.stringify(tuples, undefined, 2)}\n`,
			catch: (cause) =>
				new TandemServerJsonFileStorageError({
					operation: "serialize",
					filePath: this.args.filePath,
					cause,
				}),
		})
		if (serialized instanceof Error) return serialized

		const directory = await runFileOperation({
			operation: "create directory for",
			filePath: this.args.filePath,
			promise: fs.mkdir(path.dirname(this.args.filePath), { recursive: true }),
		})
		if (directory instanceof Error) return directory

		const temporaryFilePath = `${this.args.filePath}.${crypto.randomUUID()}.tmp`
		await using cleanup = new errore.AsyncDisposableStack()
		cleanup.defer(async () => {
			const removed = await runFileOperation({
				operation: "clean up temporary file for",
				filePath: this.args.filePath,
				promise: fs.rm(temporaryFilePath, { force: true }),
			})
			if (removed instanceof Error) console.warn(removed)
		})

		const written = await runFileOperation({
			operation: "write",
			filePath: this.args.filePath,
			promise: fs.writeFile(temporaryFilePath, serialized),
		})
		if (written instanceof Error) return written

		const renamed = await runFileOperation({
			operation: "publish",
			filePath: this.args.filePath,
			promise: fs.rename(temporaryFilePath, this.args.filePath),
		})
		if (renamed instanceof Error) return renamed
	}
}
