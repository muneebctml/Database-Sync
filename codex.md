# Codex Instructions for This Repo

This repository contains a multi-project .NET 10 solution for a cross‑database sync CLI called **DbSync**.

Future AI agents working here should follow these guidelines before making changes.

## 1. Solution & Projects

- Main solution to open: `DbSync.sln`
- Projects:
  - `src/DbSync.Cli` – CLI entry point, Spectre.Console commands, DI/hosting configuration.
  - `src/DbSync.Core` – canonical schema model, provider abstractions, diff engine, migration plan.
  - `src/DbSync.Engine` – sync engine, migration executor, options/config binding.
  - `src/DbSync.AI` – AI advisor abstractions (`IAiAdvisor`), `NullAiAdvisor`, `McpAiAdvisor` stub.
  - `src/DbSync.Providers.SqlServer` – SQL Server provider (introspection, DDL, SqlBulkCopy writer).
  - `src/DbSync.Providers.Postgres` – PostgreSQL provider (introspection, DDL, batched insert writer).
  - `tests/DbSync.Core.Tests` – xUnit tests for the diff engine and core logic.

Always hook new functionality into this structure instead of creating ad‑hoc projects.

## 2. Build, Run, Test

- Build everything:
  - `dotnet build DbSync.sln`
- Run tests:
  - `dotnet test DbSync.sln`
- Run CLI (development):
  - `dotnet run --project src/DbSync.Cli -- wizard`

Agents must ensure the solution builds and tests pass after changes, unless the user explicitly says otherwise.

## 3. CLI Design Expectations

- Main commands (in `DbSync.Cli`):
  - `wizard`: primary interactive path (prompt for source/target, diff, migrate, sync).
  - `diff`: non‑interactive schema diff, with optional `--json` output.
  - `migrate`: compute diff and optionally apply to target (`--apply` or prompt).
  - `sync`: data synchronization (`--mode full|append`, `--batch-size`).
  - `target truncate/delete/update`: direct operations on target with confirmation and safety checks.
- Use Spectre.Console for interactions (prompts, tables, status).
- Never print passwords or secrets; always use masked inputs.

When adding new commands or options, keep naming consistent and avoid breaking existing behavior unless the user explicitly requests a breaking change.

## 4. Core & Provider Architecture

- Canonical schema lives in `DbSync.Core.Schema`:
  - `DatabaseSchema`, `TableSchema`, `ColumnSchema`, `PrimaryKeySchema`, `CanonicalDataType`.
- Provider abstractions in `DbSync.Core.Providers`:
  - `IProvider`, `IDbSession`, `IIntrospector`, `IDdlGenerator`, `IDataReader`, `IDataWriter`, `ICapabilities`, `RowData`.
- Diff engine in `DbSync.Core.Diff`:
  - `SchemaDiffEngine`, `SchemaDiffResult`, `MigrationPlan`, `MigrationStep`, `RiskLevel`, etc.

When extending providers (e.g., add MySQL/Oracle/ClickHouse/MongoDB later), **do not** change the core abstractions without a strong reason and user approval. Prefer adding new provider projects mirroring the SQL Server/Postgres structure.

## 5. Sync Engine & Behavior

- Sync logic is in `DbSync.Engine`:
  - `ISyncEngine`, `DatabaseSyncEngine`, `SyncMode` (`Full`, `Append`), `SyncOptions`, `SyncProgress`.
  - `IMigrationExecutor`, `MigrationExecutor`.
  - `DbSyncOptions` (`DbSync` section in config).
- Requirements:
  - Full mode: truncate target tables (if supported) then reload all data.
  - Append mode: insert without truncation.
  - Use streaming reads (`IAsyncEnumerable`) and batched writes; batch size configurable.
  - Provide progress updates (per table and overall) through `SyncProgress`.

Any changes to sync behavior should preserve these semantics unless the user explicitly requests different behavior.

## 6. AI Advisor Mode

- AI is optional and must **never** be required for correctness.
- `DbSync.AI` defines:
  - `IAiAdvisor`, `AiSqlSuggestion`.
  - `NullAiAdvisor` – default, returns no suggestions.
  - `McpAiAdvisor` – stub that prints JSON descriptions of migration steps to STDOUT (MCP‑style), without blocking for responses.
- `DbSync.Engine` has `MigrationAiAugmentor` that enriches `MigrationStep` with AI SQL + reasoning when enabled.
- Config is controlled via:
  - `DbSync:Ai:Enabled` (bool)
  - `DbSync:Ai:Mode` (`None` / `Mcp`, etc.)

When modifying AI features:
- Do not introduce hard dependencies on external services/packages.
- Ensure the tool still works fully with AI disabled.

## 7. Safety & UX Rules

- Never log or echo plaintext passwords or sensitive connection strings.
- Confirm before:
  - Truncating tables.
  - Applying migrations.
  - Delete/update operations; reject empty `WHERE` clauses for delete/update.
- Use colorized Spectre.Console output for:
  - Diff summaries.
  - Migration plans.
  - Progress and status messages.

When in doubt, favor safer behavior (extra confirmation) over convenience.

## 8. Code Style & Changes

- Target framework: `net10.0` across all projects.
- Use existing namespace patterns:
  - `DbSync.Cli`, `DbSync.Core.*`, `DbSync.Engine`, `DbSync.AI`, `DbSync.Providers.*`.
- Prefer:
  - `async`/`await` for I/O.
  - Cancellation tokens on public async APIs.
  - Small, focused classes; avoid “god objects”.
- Keep changes minimal and cohesive with the current design; avoid large refactors unless explicitly requested.

## 9. What to Do When Asked for New Features

When a future user asks for new functionality:

1. **Read this file** (`codex.md`) and `README.md` to understand intent and constraints.
2. Identify the right project(s) to change (CLI, Core, Engine, provider, AI).
3. Update or extend existing abstractions rather than introducing parallel ones.
4. Keep the CLI experience coherent (commands, options, prompts).
5. Run `dotnet build DbSync.sln` and, when appropriate, `dotnet test DbSync.sln`.
6. Summarize changes clearly for the user, including any behavioral impacts.

If a request conflicts with these guidelines, prioritize explicit user instructions but call out the trade‑offs in your explanation.

