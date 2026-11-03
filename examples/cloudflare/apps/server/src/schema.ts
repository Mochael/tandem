import {
	collection,
	defineSchema,
	type RemoteApi,
} from "@tanishqkancharla/tandem-core"

export type Task = { id: string; text: string; done: boolean }
export type AppSchema = { tasks: Task }

export const schema = defineSchema({
	tasks: collection<Task>({ fields: ["id", "text", "done"] }),
})

export type SyncRequest =
	| { action: "push"; args: Parameters<RemoteApi<AppSchema>["push"]>[0] }
	| { action: "pull"; args: Parameters<RemoteApi<AppSchema>["pull"]>[0] }
