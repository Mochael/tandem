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
- **Tuple Model**: `TandemClientTuple<Schema>` includes `["record", collection, ...id]` records, `["replica"]` metadata (`clientId`, `lastIssuedMutationId`, optional `cookie`), and `["outbox", mutationId]` ordered mutation operations. All use the same `scan` and `commit` methods.
- **Caching**: In-memory cache with write-through
- **Batching**: Groups writes for better performance

## Key implementation notes

- Single IndexedDB object store for all data
- Composite indexes for efficient querying
- Automatic retry with exponential backoff
- Graceful fallback to in-memory storage on errors
- ACID properties maintained through transactions

IndexedDB applies collection codecs to record values and to nested `set` values in outbox operations. Metadata and `remove` operations pass through unchanged. Encoding completes before opening the write transaction, so a codec failure cannot persist a prefix of the batch.

The tuple schema is storage groundwork only: `SyncEngine` does not yet write or hydrate replica metadata or outbox entries. `Database` loads only the `record` subspace and still persists its visible records. Durable sync state and confirmed-only record persistence are subsequent work.
