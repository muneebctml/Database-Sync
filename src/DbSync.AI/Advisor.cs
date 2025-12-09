using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbSync.Core.Diff;

namespace DbSync.AI;

public sealed class AiSqlSuggestion
{
    public AiSqlSuggestion(string? sql, string? reasoning)
    {
        Sql = sql;
        Reasoning = reasoning;
    }

    public string? Sql { get; }

    public string? Reasoning { get; }
}

public interface IAiAdvisor
{
    bool IsEnabled { get; }

    Task<AiSqlSuggestion?> SuggestMigrationAsync(MigrationStep step, CancellationToken cancellationToken = default);
}

public sealed class NullAiAdvisor : IAiAdvisor
{
    public bool IsEnabled => false;

    public Task<AiSqlSuggestion?> SuggestMigrationAsync(MigrationStep step, CancellationToken cancellationToken = default)
    {
        if (step is null) throw new ArgumentNullException(nameof(step));
        return Task.FromResult<AiSqlSuggestion?>(null);
    }
}

/// <summary>
/// Stub MCP-style advisor that writes JSON requests to standard output.
/// It does not block waiting for a response, so it is safe to enable even
/// when no companion process is attached.
/// </summary>
public sealed class McpAiAdvisor : IAiAdvisor
{
    private readonly JsonSerializerOptions _serializerOptions = new(JsonSerializerDefaults.Web);

    public bool IsEnabled => true;

    public async Task<AiSqlSuggestion?> SuggestMigrationAsync(MigrationStep step, CancellationToken cancellationToken = default)
    {
        if (step is null) throw new ArgumentNullException(nameof(step));

        var request = new
        {
            type = "migration_suggestion",
            schema = step.SchemaName,
            table = step.TableName,
            sql = step.Sql,
            risk = step.Risk.ToString()
        };

        var json = JsonSerializer.Serialize(request, _serializerOptions);
        await Console.Out.WriteLineAsync(json);
        await Console.Out.FlushAsync();

        // Stub implementation: do not attempt to read a response.
        return null;
    }
}

