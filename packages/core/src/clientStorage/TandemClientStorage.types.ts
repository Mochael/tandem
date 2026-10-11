import {
	TandemClient,
	tag,
	type ClientId,
	type MutationId,
	type Cookie,
} from "@tanishqkancharla/tandem-core"
import type { TandemClientStorageApi } from "@tanishqkancharla/tandem-core"

type AppSchema = {
	users: { id: string; name: string }
}

type OtherSchema = {
	threads: { id: number; title: string }
}

declare const appStorage: TandemClientStorageApi<AppSchema>
declare const otherStorage: TandemClientStorageApi<OtherSchema>

new TandemClient<AppSchema, {}>({ clientStorage: appStorage })

// @ts-expect-error A client cache must store tuples for the client's schema.
new TandemClient<AppSchema, {}>({ clientStorage: otherStorage })

void appStorage.commit({
	set: [
		{
			key: ["replica"],
			value: {
				clientId: tag<ClientId>("client"),
				lastIssuedMutationId: tag<MutationId>(2),
				cookie: tag<Cookie>("view"),
			},
		},
		{
			key: ["outbox", tag<MutationId>(2)],
			value: [
				{
					type: "set",
					collection: "users",
					value: { id: "user", name: "Ada" },
				},
				{ type: "remove", collection: "users", id: "old-user" },
			],
		},
	],
})

void appStorage.commit({
	set: [
		{
			key: ["outbox", tag<MutationId>(3)],
			value: [
				{
					type: "set",
					// @ts-expect-error Outbox operations must name an application collection.
					collection: "threads",
					value: { id: "user", name: "Ada" },
				},
			],
		},
	],
})
