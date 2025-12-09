using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbSync.Core.Diff;
using DbSync.Core.Providers;
using Microsoft.Extensions.Logging;
using Spectre.Console;
using Spectre.Console.Cli;

namespace DbSync.Cli;

public abstract class ConnectionSettings : CommandSettings
{
    [CommandOption("--source-provider <PROVIDER>")]
    public string SourceProvider { get; set; } = string.Empty;

    [CommandOption("--source-host <HOST>")]
    public string SourceHost { get; set; } = "localhost";

    [CommandOption("--source-port <PORT>")]
    public int? SourcePort { get; set; }

    [CommandOption("--source-username <USERNAME>")]
    public string SourceUsername { get; set; } = string.Empty;

    [CommandOption("--source-password <PASSWORD>")]
    public string SourcePassword { get; set; } = string.Empty;

    [CommandOption("--source-use-ssl")]
    public bool SourceUseSsl { get; set; }

    [CommandOption("--source-database <DATABASE>")]
    public string? SourceDatabase { get; set; }

    [CommandOption("--target-provider <PROVIDER>")]
    public string TargetProvider { get; set; } = string.Empty;

    [CommandOption("--target-host <HOST>")]
    public string TargetHost { get; set; } = "localhost";

    [CommandOption("--target-port <PORT>")]
    public int? TargetPort { get; set; }

    [CommandOption("--target-username <USERNAME>")]
    public string TargetUsername { get; set; } = string.Empty;

    [CommandOption("--target-password <PASSWORD>")]
    public string TargetPassword { get; set; } = string.Empty;

    [CommandOption("--target-use-ssl")]
    public bool TargetUseSsl { get; set; }

    [CommandOption("--target-database <DATABASE>")]
    public string? TargetDatabase { get; set; }
}

public sealed class DiffCommandSettings : ConnectionSettings
{
    [CommandOption("--json")]
    public bool Json { get; set; }
}

public sealed class DiffCommand : AsyncCommand<DiffCommandSettings>
{
    private readonly ProviderRegistry _providers;
    private readonly ILogger<DiffCommand> _logger;

    public DiffCommand(ProviderRegistry providers, ILogger<DiffCommand> logger)
    {
        _providers = providers ?? throw new ArgumentNullException(nameof(providers));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public override async Task<int> ExecuteAsync(CommandContext context, DiffCommandSettings settings)
    {
        var cancellationToken = CancellationToken.None;

        try
        {
            var sourceKind = ParseProviderKind(settings.SourceProvider);
            var targetKind = ParseProviderKind(settings.TargetProvider);

            var sourceProvider = _providers.GetByKind(sourceKind);
            var targetProvider = _providers.GetByKind(targetKind);

            if (string.IsNullOrWhiteSpace(settings.SourceDatabase) ||
                string.IsNullOrWhiteSpace(settings.TargetDatabase))
            {
                throw new InvalidOperationException("Both --source-database and --target-database must be provided for non-interactive diff.");
            }

            var sourceConnection = new ConnectionInfo(
                sourceKind,
                settings.SourceHost,
                settings.SourcePort,
                settings.SourceUsername,
                settings.SourcePassword,
                UseIntegratedSecurity: false,
                UseSsl: settings.SourceUseSsl);

            var targetConnection = new ConnectionInfo(
                targetKind,
                settings.TargetHost,
                settings.TargetPort,
                settings.TargetUsername,
                settings.TargetPassword,
                UseIntegratedSecurity: false,
                UseSsl: settings.TargetUseSsl);

            await using var sourceSession = await sourceProvider.ConnectAsync(sourceConnection, settings.SourceDatabase!, cancellationToken)
                .ConfigureAwait(false);
            await using var targetSession = await targetProvider.ConnectAsync(targetConnection, settings.TargetDatabase!, cancellationToken)
                .ConfigureAwait(false);

            var sourceSchema = await sourceSession.Introspector.GetDatabaseSchemaAsync(cancellationToken).ConfigureAwait(false);
            var targetSchema = await targetSession.Introspector.GetDatabaseSchemaAsync(cancellationToken).ConfigureAwait(false);

            var diffResult = SchemaDiffEngine.Diff(sourceSchema, targetSchema, targetSession.DdlGenerator);

            if (settings.Json)
            {
                var json = JsonSerializer.Serialize(diffResult, new JsonSerializerOptions(JsonSerializerDefaults.Web)
                {
                    WriteIndented = true
                });
                Console.WriteLine(json);
            }
            else
            {
                WizardCommand.RenderDiffSummary(diffResult);
            }

            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Diff command failed.");
            AnsiConsole.MarkupLine($"[red]Error:[/] {ex.Message}");
            return -1;
        }
    }

    public static ProviderKind ParseProviderKind(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException("Provider must be specified.");
        }

        return value.ToLowerInvariant() switch
        {
            "sqlserver" => ProviderKind.SqlServer,
            "postgres" or "postgresql" => ProviderKind.Postgres,
            _ => throw new InvalidOperationException($"Unknown provider '{value}'. Expected 'sqlserver' or 'postgres'.")
        };
    }
}
