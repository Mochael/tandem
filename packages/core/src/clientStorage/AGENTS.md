# Tandem Client Storage

## What

Persistent tuple storage with IndexedDB backend. Handles data persistence, caching, and provides storage abstraction for the database layer.

## How to use

```typescript
import { TandemClientIndexedDbStorage } from "./TandemClientIndexedDbStorage.js";

const storage = new TandemClientIndexedDbStorage<Schema>({
	dbName: "my-app",
	schema,
});

// Used internally by TandemClient
const db = new TandemClient({ schema, clientStorage: storage });
```

## How it works

- **IndexedDB**: Browser persistent storage
- **Tuple Model**: `TandemClientTuple<Schema>` includes `["record", collection, ...id]` records, `["base", collection, ...id]` sparse server values and `lastWrittenBy`, `["replica"]` metadata (`clientId`, `lastIssuedMutationId`, optional `cookie`), and `["outbox", mutationId]` ordered mutation operations. All use the same `scan` and `commit` methods.
- **Caching**: In-memory cache with write-through
- **Batching**: Groups writes for better performance

## Key implementation notes

- Single IndexedDB object store for all data
- Each adapter commit must apply its whole batch atomically and resolve only after it is durable
- Serialized batches preserve transaction order; explicit flush waits for queued and active writes
- Failed batches remain buffered before newer writes; a later commit or explicit flush can retry them
- Storage failure rejects the waiting public operation; there is no automatic backoff or in-memory fallback

IndexedDB applies collection codecs to record values and to nested `set` values in bases and outbox operations. Metadata and `remove` markers pass through unchanged. Encoding completes before opening the write transaction, so a codec failure cannot persist a prefix of the batch.

`Database` commits records, bases, outbox entries, and replica metadata together in memory, then persists root tuple writes through the same storage API. Pushes wait for their complete mutation snapshot to reach storage. Pull checkpoints include the patch, acknowledgement pruning, and cookie. Clear runs after active saves and supersedes queued old-session writes.

Startup loads every checkpoint namespace before installing the persistence subscriber. It preserves the saved identity, cookie, and mutation counter, initializing replica metadata only for a fresh store. Stored records already contain optimistic edits, so startup does not replay them again. `TandemClient.ready` resolves after hydration; transactions throw before readiness, and clear and connection wait for it. A scan failure rejects readiness without overwriting the saved checkpoint.

Reconnect pulls first to prune mutations already accepted by the server, then pushes the remaining outbox with its original IDs. Durable offline edits survive client restart. This assumes one active client owns the store; shared-store coordination and automatic retry remain outside this implementation.
