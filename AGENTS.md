# Indexer — Agent Guidelines

## Purpose

`github.com/theleeeo/indexer` is a **generic search-index synchronisation engine**. It bridges arbitrary backend microservices (via pluggable gRPC "Provider" plugins) with an Elasticsearch cluster, keeping search indices fresh as data changes. External services notify it via gRPC when resources change; the indexer determines which denormalised ES documents are affected, rebuilds them, and writes them to ES. A [River](https://riverqueue.com)-backed Postgres job queue provides at-least-once processing.

The core parts of the library are also available to be used as a library for users who need more control over all requests and handling of the resources.

### Data Flow

```
External microservice
  │  NotifyChange / NotifyChangeBatch  (gRPC → IndexService, optional metadata)
       ▼
server/ — translates proto → source.Notification
       ▼
core/Indexer.RegisterChange
       │  UpsertResource / DeleteResource    → Postgres (resources table)
       │  AffectedRoots ←                   ← Postgres (relations table)
  │  riverClient.Insert(RebuildArgs/DeleteArgs)   → River (river_job table)
       ▼
River Client workers (core/RebuildWorker, DeleteWorker)
       ▼
core/handleRebuild
      │  projection/Builder.Build
      │    ├─ aggregation/RootPlan  → provider.FetchResource  (gRPC → ProviderService, metadata)
      │    └─ aggregation/SubPlan   → provider.FetchRelated   (gRPC → ProviderService, metadata)
       │  store.AddChildResources    → Postgres (relations)
       │  es.Client.Upsert           → Elasticsearch
       ▼
      Done

Full rebuild path:
Client → server/IndexerServer.Rebuild → core/Indexer.Rebuild
       │  Validate selectors
       │  riverClient.Insert(FullRebuildArgs) → River
       ▼
core/FullRebuildWorker → handleFullRebuild
       │  If resource_ids given: handleRebuildVersions per ID
       │  If empty: execute plan with ResourceID="" per version
       │    └─ plan streams pages of BuildDocs via provider.ListResources internally
       │    └─ per BuildDoc: upsert ES, collect relations
       │  Persist relation union for all discovered resources

Search path:
Client → server/SearcherServer → core/Indexer.Search → es/Client.Search → Elasticsearch
```

---

## Architecture: Package Reference

### `model/`

Single shared identity type: `Resource{Type, Id}` used across all layers as the universal key for a resource instance.

### `resource/`

YAML DSL and runtime config. `Config` describes one resource via its `Versions []VersionConfig` slice, where each `VersionConfig` holds a `Version int`, `[]FieldConfig`, and `[]RelationConfig` specifying how to hydrate related resources (which fields to pull, cardinality, key sources). `ReadVersion int` indicates which version the read alias points to. Utility methods: `SortedVersions()`, `GetVersion(v)`, `ReadVersionConfig()`, `HasRelationTo(type)`. `Validate()` checks for inter-resource consistency (no cycles, valid field references). Config path defaults to `resources.yml` and can be set via app config file or `RESOURCE_CONFIG_PATH` env override.

Key types: `Config`, `VersionConfig`, `RelationConfig`, `FieldConfig`, `KeyConfig`.

### `store/`

Postgres relation graph. Tracks known resource instances (`resources` table) and directed parent→child dependency edges (`relations` table). The relation graph is the mechanism by which `AffectedRoots` discovers all root documents that embed a changed resource and must be reindexed.

Key types: `PostgresStore`, `Relation`.  
Schema: [store/pg_schema.sql](store/pg_schema.sql).

### Job queue

The indexer uses [River](https://riverqueue.com) (`github.com/riverqueue/river`) as its durable Postgres job queue. Job args + workers live in [core/worker.go](core/worker.go):

- `RebuildArgs` / `RebuildWorker` — incremental root rebuild.
- `DeleteArgs` / `DeleteWorker` — delete a root document from ES and remove relations.
- `FullRebuildArgs` / `FullRebuildWorker` — full rebuild of a resource type, optionally scoped to specific IDs and/or versions.

There is **no per-resource serialization guarantee**: concurrent rebuilds of the same root can run in parallel. ES upserts are idempotent, and `store.AddChildResources` converges relation state. Callers should expect eventual consistency under bursts.

River's schema is applied at startup via `rivermigrate` — there is no local SQL schema file to apply manually. Use [riverui](https://github.com/riverqueue/riverui) or River's own Go API for operational inspection (listing, counts, error summaries).

### `source/`

Inbound and outbound data contracts.

- `Notification{ResourceType, ResourceID, Kind, Metadata}` — what changed (`ChangeCreated`, `ChangeUpdated`, `ChangeDeleted`) plus arbitrary caller-provided key-value context.
- `Provider` interface — `FetchResource`, `FetchRelated`, and `ListResources`. All live data retrieval goes through this boundary.
- `GRPCProvider` — implements `Provider` by calling a remote `ProviderService` plugin.
- `ListResourcesParams` / `ListResourcesResult` / `ListedResource` — paginated listing types for full rebuilds.

### `aggregation/`

Generic streaming pipeline primitives.

- `RootPlan` — fetches a root resource with pagination support, streams results.
- `SubPlan` — wraps a parent plan, calls a sub-fetcher per parent item, merges results. All stages run concurrently over channels.

### `projection/`

Builds denormalised ES documents for a root resource.

- `Builder.Build(ctx, type, id)` — runs the full pipeline (root + all relations in topological order), persists relation edges to PG, returns a `map[string]any` document ready for ES.
- `Builder.AffectedRoots(ctx, type, id)` — traverses PG to find all root resources that embed the changed resource.
- `BuildPlansFromConfig` — constructs the ordered plan chain (RootPlan → SubPlan per relation) from `resource.Config`.

Key types: `Builder`, `BuildDoc`.

### `core/`

Central orchestrator (`Indexer` struct).

- `RegisterChange` — inbound change handler: updates PG, finds affected roots, enqueues jobs.
- `handleRebuild` / `handleDelete` — job handlers; rebuild calls `Builder.Build` then ES upsert; delete calls ES delete and removes PG records.
- `handleFullRebuild` — job handler for `"full_rebuild"` jobs; for specific IDs calls `handleRebuildVersions` per resource; for "rebuild all" executes each version's plan with `ResourceID=""` which streams all resources via the plan's internal `ListResources` pagination.
- `Rebuild` — validates selectors, enqueues one `FullRebuildArgs` River job per `ResourceSelector`.
- `RegisterWorkers(workers, idx)` — registers the three River workers (`rebuild`, `delete`, `full_rebuild`) against a `river.Workers` registry so callers can pass it to `river.NewClient`.
- `SetRiverClient(client)` — assigns the River client used to enqueue jobs. Needed because workers reference the `Indexer`, so the client is created after the `Indexer` and wired back in.
- `Search` — validates resource type, caps pagination, delegates to ES.
- `GetCapabilities` — returns the search capabilities (available resources, filterable fields, supported operations) derived from the resource configs.

### `es/`

Elasticsearch client wrapper (`Client`). Methods: `Upsert`, `Delete`, `BulkUpsert`, `Get`, `Search`. The `Search` method composes a bool query with optional `multi_match` full-text, `term`/`terms` filter clauses, and sort.

`GenerateMapping` / `GenerateMappings` — derives ES index mappings from `resource.Config` (relations with cardinality `"one"` → ES `object`; otherwise → `nested`).

### `server/`

gRPC server adapters (thin protocol translation only, no business logic).

- `IndexerServer` — implements `IndexService`: translates proto → `source.Notification`, calls `core.Indexer.RegisterChange`. Also handles the `Rebuild` RPC.
- `SearcherServer` — implements `SearchService`: delegates to `core.Indexer.Search` and `core.Indexer.GetCapabilities`.

### `cmd/indexer/`

Main binary. Loads runtime settings from an app config file (`indexer.yml` by default, or `APP_CONFIG_PATH`) with env var overrides:

| Env var                       | Purpose                                         |
| ----------------------------- | ----------------------------------------------- |
| `APP_CONFIG_PATH`             | Path to app config file (default `indexer.yml`) |
| `RESOURCE_CONFIG_PATH`        | Path to `resources.yml`                         |
| `ES_ADDRS`                    | Elasticsearch addresses                         |
| `ES_USERNAME` / `ES_PASSWORD` | ES credentials                                  |
| `PG_ADDR`                     | Postgres connection string                      |
| `PROVIDER_ADDR`               | gRPC address of the provider plugin             |
| `GRPC_ADDR`                   | Listening address (default `:9000`)             |

### `cmd/gen-mapping/`

CLI tool. Reads resource config, generates ES index mappings, and optionally `PUT`s them to a live cluster (`-apply` flag).

---

## gRPC Services

| Proto                              | Service                              | Key Messages                                                                                                                                                                                                                             |
| ---------------------------------- | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `proto/index/v1/index.proto`       | `IndexService` (inbound)             | `ChangeNotification{kind, resource_type, resource_id, metadata}`, `RebuildRequest{repeated ResourceSelector}`, `RebuildResponse`                                                                                                         |
| `proto/provider/v1/provider.proto` | `ProviderService` (plugin, outbound) | `FetchResourceRequest{..., metadata}` / `Response`, `FetchRelatedRequest{..., metadata}` / `Response`, `ListResourcesRequest{..., metadata}` / `Response` (data as `google.protobuf.Struct`)                                             |
| `proto/search/v1/search.proto`     | `SearchService`                      | `SearchRequest{resource, query, filters, page, page_size, sort}`, `SearchResponse{total, hits}`, `GetCapabilitiesRequest`, `GetCapabilitiesResponse{resources: []{resource, fields: []{field, type, filter_ops, searchable, sortable}}}` |

Generated code lives in `gen/`. Regenerate with `buf generate`.

---

## Resource Config DSL

Resources are defined in `resources.yml`. Each entry in the flat `resources:` list defines a single version of a resource type. Multiple entries with the same `type` but different `version` numbers define multiple versions (for zero-downtime schema migrations). The `readVersion` field controls which version the read alias points to.

```yaml
resources:
  - type: myResource
    version: 1
    readVersion: 1
    fields:
      - name: title
        type: text
        query:
          search: true # included in multi_match full-text search
      - name: status
    relations:
      - resource: otherResource
        cardinality: one # "one" → ES object, anything else → ES nested
        key:
          source: myResource # which resolved resource holds the FK
          field: other_id # field name in source doc
        fields:
          - name: label

  - type: myResource
    version: 2
    fields:
      fields:
        - name: title
          type: text
        - name: status
        - name: newField
      relations:
        - resource: otherResource
          cardinality: one
          key:
            source: myResource
            field: other_id
          fields:
            - name: label
```

Key chaining: a relation's `key.source` may reference another relation's resource name, enabling `A → B → C` fetch chains processed in topological order.

---

## Build & Test

```bash
# Run the main binary
GOEXPERIMENT=jsonv2 go run ./cmd/indexer

# Generate ES mappings
go run ./cmd/gen-mapping -config resources.yml

# Regenerate proto bindings
buf generate

# Run tests (uses testcontainers — requires Docker)
go test ./...
```

> **Important:** All builds require `GOEXPERIMENT=jsonv2` because the codebase uses `encoding/json/v2`.

---

## Conventions

- **No per-resource serialization:** rebuilds of the same root may run concurrently under bursts. ES upserts are idempotent and `store.AddChildResources` converges; callers should expect eventual consistency. If strict ordering is needed later, add an advisory-lock guard inside the rebuild worker or use River's `UniqueOpts`.
- **Relation graph is the source of truth for "what to reindex":** when a child resource changes, `AffectedRoots` is the mechanism for finding parent documents — not a config lookup.
- **ES mappings are derived from config:** use `gen-mapping` after changing resource configs; do not hand-edit ES mappings.
- **BuildRequest conventions:** `BuildRequest.ResourceID == ""` triggers "get all" mode in the root plan fetcher, which calls `provider.ListResources` with pagination. A non-empty `ResourceID` fetches a single resource.
- Always modify the AGENTS.md file if anything is changed that would cause anything currently written to be incorrect.
- **Runs distributed** All implementations must work even when this application/library in running as multiple instances.
- All additions or changes must be reflected in the tests. No feature can be untested.
