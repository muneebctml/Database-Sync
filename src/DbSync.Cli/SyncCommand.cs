using System;
using System.Threading;
using System.Threading.Tasks;
using DbSync.Engine;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Spectre.Console;
using Spectre.Console.Cli;

namespace DbSync.Cli;

public sealed class SyncCommandSettings : ConnectionSettings
{
    [CommandOption("--mode <MODE>")]
    public string Mode { get; set; } = "full";

    [CommandOption("--batch-size <SIZE>")]
    public int? BatchSize { get; set; }
}

public sealed class SyncCommand : AsyncCommand<SyncCommandSettings>
{
    private readonly ProviderRegistry _providers;
    private readonly ISyncEngine _syncEngine;
    private readonly DbSyncOptions _options;
    private readonly ILogger<SyncCommand> _logger;

    public SyncCommand(
        ProviderRegistry providers,
        ISyncEngine syncEngine,
        IOptions<DbSyncOptions> options,
        ILogger<SyncCommand> logger)
    {
        _providers = providers ?? throw new ArgumentNullException(nameof(providers));
        _syncEngine = syncEngine ?? throw new ArgumentNullException(nameof(syncEngine));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public override async Task<int> ExecuteAsync(CommandContext context, SyncCommandSettings settings)
    {
        var cancellationToken = CancellationToken.None;

        try
        {
            var sourceKind = DiffCommand.ParseProviderKind(settings.SourceProvider);
            var targetKind = DiffCommand.ParseProviderKind(settings.TargetProvider);

            var sourceProvider = _providers.GetByKind(sourceKind);
            var targetProvider = _providers.GetByKind(targetKind);

            if (string.IsNullOrWhiteSpace(settings.SourceDatabase) ||
                string.IsNullOrWhiteSpace(settings.TargetDatabase))
            {
                throw new InvalidOperationException("Both --source-database and --target-database must be provided for non-interactive sync.");
            }

            var mode = settings.Mode.ToLowerInvariant() switch
            {
                "full" => SyncMode.Full,
                "append" => SyncMode.Append,
                _ => throw new InvalidOperationException("Mode must be 'full' or 'append'.")
            };

            if (mode == SyncMode.Full)
            {
                var confirm = AnsiConsole.Confirm(
                    "Full mode will [red]truncate all matching tables on the target[/] before loading. Continue?",
                    defaultValue: false);
                if (!confirm)
                {
                    AnsiConsole.MarkupLine("[yellow]Sync cancelled by user.[/]");
                    return 0;
                }
            }

            var batchSize = settings.BatchSize ?? (_options.DefaultBatchSize <= 0 ? 5000 : _options.DefaultBatchSize);

            var sourceConnection = new DbSync.Core.Providers.ConnectionInfo(
                sourceKind,
                settings.SourceHost,
                settings.SourcePort,
                settings.SourceUsername,
                settings.SourcePassword,
                UseIntegratedSecurity: false,
                UseSsl: settings.SourceUseSsl);

            var targetConnection = new DbSync.Core.Providers.ConnectionInfo(
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

            var options = new SyncOptions
            {
                Mode = mode,
                BatchSize = batchSize
            };

            var progress = new Progress<SyncProgress>(p =>
            {
                AnsiConsole.MarkupLine(
                    $"[grey]Synced[/] [green]{p.RowsSynced}[/] rows for [yellow]{p.Table}[/] " +
                    $"([green]{p.TablesCompleted}/{p.TotalTables}[/] tables)");
            });

            await _syncEngine.SyncAsync(
                    sourceSession,
                    targetSession,
                    sourceSchema,
                    targetSchema,
                    options,
                    progress,
                    cancellationToken)
                .ConfigureAwait(false);

            AnsiConsole.MarkupLine("[green]Data sync completed.[/]");
            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Sync command failed.");
            AnsiConsole.MarkupLine($"[red]Error:[/] {ex.Message}");
            return -1;
        }
    }
}
