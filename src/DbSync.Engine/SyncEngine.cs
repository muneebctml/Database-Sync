using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbSync.AI;
using DbSync.Core.Diff;
using DbSync.Core.Providers;
using DbSync.Core.Schema;

namespace DbSync.Engine;

public enum SyncMode
{
    Full,
    Append
}

public sealed class SyncOptions
{
    public SyncMode Mode { get; init; } = SyncMode.Full;

    public int BatchSize { get; init; } = 5000;
}

public sealed class SyncProgress
{
    public string Table { get; init; } = string.Empty;

    public SyncMode Mode { get; init; }

    public long RowsSynced { get; init; }

    public int TablesCompleted { get; init; }

    public int TotalTables { get; init; }
}

public interface ISyncEngine
{
    Task SyncAsync(
        IDbSession sourceSession,
        IDbSession targetSession,
        DatabaseSchema sourceSchema,
        DatabaseSchema targetSchema,
        SyncOptions options,
        IProgress<SyncProgress>? progress = null,
        CancellationToken cancellationToken = default);
}

public sealed class DatabaseSyncEngine : ISyncEngine
{
    public async Task SyncAsync(
        IDbSession sourceSession,
        IDbSession targetSession,
        DatabaseSchema sourceSchema,
        DatabaseSchema targetSchema,
        SyncOptions options,
        IProgress<SyncProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (sourceSession is null) throw new ArgumentNullException(nameof(sourceSession));
        if (targetSession is null) throw new ArgumentNullException(nameof(targetSession));
        if (sourceSchema is null) throw new ArgumentNullException(nameof(sourceSchema));
        if (targetSchema is null) throw new ArgumentNullException(nameof(targetSchema));
        if (options is null) throw new ArgumentNullException(nameof(options));

        var batchSize = options.BatchSize <= 0 ? 5000 : options.BatchSize;

        var targetTables = targetSchema.Tables.ToDictionary(
            t => (Schema: t.SchemaName, Table: t.TableName));

        var tablesToSync = sourceSchema.Tables
            .Where(t => targetTables.ContainsKey((t.SchemaName, t.TableName)))
            .ToList();

        var totalTables = tablesToSync.Count;
        var completedTables = 0;

        foreach (var sourceTable in tablesToSync)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var targetTable = targetTables[(sourceTable.SchemaName, sourceTable.TableName)];

            if (options.Mode == SyncMode.Full && targetSession.Capabilities.SupportsTruncate)
            {
                await targetSession.DataWriter.TruncateTableAsync(targetTable, cancellationToken).ConfigureAwait(false);
            }

            var rowsSyncedForTable = 0L;
            var batch = new List<RowData>(batchSize);

            await foreach (var row in sourceSession.DataReader.ReadTableAsync(sourceTable, cancellationToken).ConfigureAwait(false))
            {
                batch.Add(row);

                if (batch.Count >= batchSize)
                {
                    await targetSession.DataWriter.InsertBatchAsync(targetTable, batch, cancellationToken).ConfigureAwait(false);
                    rowsSyncedForTable += batch.Count;
                    batch.Clear();

                    progress?.Report(new SyncProgress
                    {
                        Table = $"{sourceTable.SchemaName}.{sourceTable.TableName}",
                        Mode = options.Mode,
                        RowsSynced = rowsSyncedForTable,
                        TablesCompleted = completedTables,
                        TotalTables = totalTables
                    });
                }
            }

            if (batch.Count > 0)
            {
                await targetSession.DataWriter.InsertBatchAsync(targetTable, batch, cancellationToken).ConfigureAwait(false);
                rowsSyncedForTable += batch.Count;
            }

            completedTables++;

            progress?.Report(new SyncProgress
            {
                Table = $"{sourceTable.SchemaName}.{sourceTable.TableName}",
                Mode = options.Mode,
                RowsSynced = rowsSyncedForTable,
                TablesCompleted = completedTables,
                TotalTables = totalTables
            });
        }
    }
}

public interface IMigrationExecutor
{
    Task ApplyMigrationAsync(
        IDbSession targetSession,
        MigrationPlan plan,
        CancellationToken cancellationToken = default);
}

public sealed class MigrationExecutor : IMigrationExecutor
{
    public async Task ApplyMigrationAsync(
        IDbSession targetSession,
        MigrationPlan plan,
        CancellationToken cancellationToken = default)
    {
        if (targetSession is null) throw new ArgumentNullException(nameof(targetSession));
        if (plan is null) throw new ArgumentNullException(nameof(plan));

        foreach (var step in plan.Steps)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var command = targetSession.Connection.CreateCommand();
            command.CommandText = step.Sql;
            command.CommandType = System.Data.CommandType.Text;

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

public sealed class DbSyncOptions
{
    public const string ConfigurationSectionName = "DbSync";

    public int DefaultBatchSize { get; set; } = 5000;

    public AiAdvisorOptions Ai { get; set; } = new();
}

public sealed class AiAdvisorOptions
{
    public bool Enabled { get; set; }

    public string Mode { get; set; } = "None";
}

public static class MigrationAiAugmentor
{
    public static async Task EnrichWithAiAsync(
        MigrationPlan plan,
        IAiAdvisor advisor,
        CancellationToken cancellationToken = default)
    {
        if (plan is null) throw new ArgumentNullException(nameof(plan));
        if (advisor is null || !advisor.IsEnabled)
        {
            return;
        }

        foreach (var step in plan.Steps)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var suggestion = await advisor.SuggestMigrationAsync(step, cancellationToken).ConfigureAwait(false);
            if (suggestion is not null)
            {
                step.AiSql = suggestion.Sql;
                step.AiReasoning = suggestion.Reasoning;
            }
        }
    }
}
