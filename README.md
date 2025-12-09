# DbSync

DbSync is a cross-database synchronization CLI for .NET 10 that can:

- Discover schemas on a source and target database
- Show schema diffs and generate deterministic migration SQL
- Optionally enrich migrations with an AI advisor (stubbed MCP integration)
- Synchronize data in `full` (truncate + reload) or `append` modes
- Perform direct operations on the target (`truncate`, `delete`, `update`)

The initial providers support SQL Server and PostgreSQL. The architecture is designed so that additional providers (MySQL, Oracle, ClickHouse, MongoDB, etc.) can be added via new projects implementing the shared abstractions in `DbSync.Core`.

## Solution Layout

- `DbSync.sln` – root solution
- `src/DbSync.Cli` – CLI entry point and commands
- `src/DbSync.Core` – canonical schema model, provider abstractions, diff engine, migration plan
- `src/DbSync.Engine` – sync engine, migration executor, options
- `src/DbSync.AI` – AI advisor abstractions, `NullAiAdvisor`, `McpAiAdvisor` stub
- `src/DbSync.Providers.SqlServer` – SQL Server provider (introspection, DDL, reader/writer)
- `src/DbSync.Providers.Postgres` – PostgreSQL provider (introspection, DDL, reader/writer)
- `tests/DbSync.Core.Tests` – xUnit tests for the diff engine

## Requirements

- .NET SDK 10.0+
- SQL Server and/or PostgreSQL instances you can connect to

## Build and Run

From the repository root:

```bash
dotnet build DbSync.sln
dotnet run --project src/DbSync.Cli -- wizard
```

This launches the interactive wizard, which walks you through:

1. Selecting a source provider (SQL Server or Postgres)
2. Entering source connection info (host, port, username, password)
3. Selecting a source database from a discovered list
4. Doing the same for the target
5. Performing schema discovery and diff
6. Optionally applying migrations to the target
7. Running a data sync in `full` or `append` mode

Passwords are always prompted with masked input and never echoed back.

## CLI Commands

All commands are rooted under the `dbsync` executable:

- `dbsync wizard` – interactive end-to-end path (recommended)
- `dbsync diff` – non-interactive schema diff
- `dbsync migrate` – non-interactive diff + optional apply
- `dbsync sync` – non-interactive data sync
- `dbsync target truncate` – truncate a table on the target
- `dbsync target delete` – delete rows from a target table with a `WHERE` clause
- `dbsync target update` – update rows on a target table with `SET` + `WHERE`

### Non-interactive examples

SQL Server → Postgres schema diff, JSON output:

```bash
dotnet run --project src/DbSync.Cli -- diff \
  --source-provider sqlserver --source-host localhost --source-username sa --source-password "<secret>" \
  --source-database SourceDb \
  --target-provider postgres --target-host localhost --target-username postgres --target-password "<secret>" \
  --target-database targetdb \
  --json
```

Apply schema migrations for the same pair (with confirmation):

```bash
dotnet run --project src/DbSync.Cli -- migrate \
  --source-provider sqlserver --source-host localhost --source-username sa --source-password "<secret>" \
  --source-database SourceDb \
  --target-provider postgres --target-host localhost --target-username postgres --target-password "<secret>" \
  --target-database targetdb \
  --apply
```

Run a full data sync (truncate + reload) with custom batch size:

```bash
dotnet run --project src/DbSync.Cli -- sync \
  --source-provider sqlserver --source-host localhost --source-username sa --source-password "<secret>" \
  --source-database SourceDb \
  --target-provider postgres --target-host localhost --target-username postgres --target-password "<secret>" \
  --target-database targetdb \
  --mode full \
  --batch-size 5000
```

Target operations, e.g., truncating a table:

```bash
dotnet run --project src/DbSync.Cli -- target truncate \
  --provider sqlserver --host localhost --username sa --password "<secret>" \
  --database TargetDb --schema dbo --table Users
```

## Configuration and AI Advisor

Configuration is loaded from `appsettings.json` in the repo root plus environment variables with the `DBSYNC_` prefix.

Default `appsettings.json`:

```json
{
  "DbSync": {
    "DefaultBatchSize": 5000,
    "Ai": {
      "Enabled": false,
      "Mode": "None"
    }
  }
}
```

Environment variables override these values using double-underscore separators, for example:

- `DBSYNC_DbSync__DefaultBatchSize=10000`
- `DBSYNC_DbSync__Ai__Enabled=true`
- `DBSYNC_DbSync__Ai__Mode=Mcp`

### AI Advisor Modes

AI is always optional. When disabled, all DDL is generated deterministically by provider-specific `IDdlGenerator` implementations.

- `NullAiAdvisor` – default implementation; returns no suggestions.
- `McpAiAdvisor` – stub implementation that writes JSON requests describing migration steps to STDOUT in a simple MCP-like shape. It does not block waiting for responses, so it is safe to enable even without a companion process.

When AI is enabled (`DbSync:Ai:Enabled=true`), the engine will pass each planned migration step to the configured advisor and, if a suggestion is returned, store its SQL + reasoning on the `MigrationStep` for display/logging.

## Extensibility

New providers are added by implementing the abstractions in `DbSync.Core`:

- `IProvider` – high-level provider; creates `IDbSession` from a `ConnectionInfo`.
- `IDbSession` – exposes `IIntrospector`, `IDdlGenerator`, `IDataReader`, `IDataWriter`, `ICapabilities`, and a live `DbConnection`.
- `IIntrospector` – builds a canonical `DatabaseSchema` from the provider’s catalogs.
- `IDdlGenerator` – generates deterministic `CREATE TABLE` and `ADD COLUMN` SQL for the provider.
- `IDataReader` / `IDataWriter` – stream rows and write batches using provider-appropriate strategies (e.g., `SqlBulkCopy`, multi-row inserts, etc.).

The canonical schema model (`DatabaseSchema`, `TableSchema`, `ColumnSchema`, `PrimaryKeySchema`, `CanonicalDataType`) is shared across providers, allowing consistent cross-database diffing and migration planning.

## Testing

Run tests from the repo root:

```bash
dotnet test DbSync.sln
```

`DbSync.Core.Tests` focuses on the diff engine, ensuring deterministic detection of missing tables/columns and generation of migration steps.

## Notes

- The codebase targets `net10.0` and is ready for Visual Studio 2026 or newer.
- The design is intentionally modular to support future providers (MySQL, Oracle, ClickHouse, MongoDB, etc.) without changing the core engine.
- All destructive operations (truncate, migrate, delete without where, update) require explicit confirmation in the CLI.
