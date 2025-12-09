using System;
using System.Threading;
using System.Threading.Tasks;
using DbSync.AI;
using DbSync.Core.Diff;
using DbSync.Core.Providers;
using DbSync.Core.Schema;
using DbSync.Engine;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Spectre.Console;
using Spectre.Console.Cli;

namespace DbSync.Cli;

public sealed class WizardSettings : CommandSettings
{
}

public sealed class WizardCommand : AsyncCommand<WizardSettings>
{
    private readonly ProviderRegistry _providers;
    private readonly ISyncEngine _syncEngine;
    private readonly IMigrationExecutor _migrationExecutor;
    private readonly IAiAdvisor _aiAdvisor;
    private readonly DbSyncOptions _options;
    private readonly ILogger<WizardCommand> _logger;

    public WizardCommand(
        ProviderRegistry providers,
        ISyncEngine syncEngine,
        IMigrationExecutor migrationExecutor,
        IAiAdvisor aiAdvisor,
        IOptions<DbSyncOptions> options,
        ILogger<WizardCommand> logger)
    {
        _providers = providers ?? throw new ArgumentNullException(nameof(providers));
        _syncEngine = syncEngine ?? throw new ArgumentNullException(nameof(syncEngine));
        _migrationExecutor = migrationExecutor ?? throw new ArgumentNullException(nameof(migrationExecutor));
        _aiAdvisor = aiAdvisor ?? throw new ArgumentNullException(nameof(aiAdvisor));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public override async Task<int> ExecuteAsync(CommandContext context, WizardSettings settings)
    {
        var cancellationToken = CancellationToken.None;

        AnsiConsole.Write(
            new FigletText("dbsync"));
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[grey]Cross-database sync wizard[/]");
        AnsiConsole.WriteLine();

        try
        {
            // Source
            var sourceKind = PromptProvider("source");
            var sourceProvider = _providers.GetByKind(sourceKind);
            var sourceConnection = PromptConnectionInfo("source", sourceKind);
            var sourceDatabase = await PromptDatabaseAsync("source", sourceProvider, sourceConnection, cancellationToken)
                .ConfigureAwait(false);

            // Target
            var targetKind = PromptProvider("target");
            var targetProvider = _providers.GetByKind(targetKind);
            var targetConnection = PromptConnectionInfo("target", targetKind);
            var targetDatabase = await PromptDatabaseAsync("target", targetProvider, targetConnection, cancellationToken)
                .ConfigureAwait(false);

            await using var sourceSession = await sourceProvider.ConnectAsync(sourceConnection, sourceDatabase, cancellationToken)
                .ConfigureAwait(false);
            await using var targetSession = await targetProvider.ConnectAsync(targetConnection, targetDatabase, cancellationToken)
                .ConfigureAwait(false);

            DatabaseSchema? sourceSchema = null;
            DatabaseSchema? targetSchema = null;

            await AnsiConsole.Status()
                .StartAsync("Discovering schemas...", async _ =>
                {
                    sourceSchema = await sourceSession.Introspector.GetDatabaseSchemaAsync(cancellationToken)
                        .ConfigureAwait(false);
                    targetSchema = await targetSession.Introspector.GetDatabaseSchemaAsync(cancellationToken)
                        .ConfigureAwait(false);
                });

            if (sourceSchema is null || targetSchema is null)
            {
                throw new InvalidOperationException("Failed to load source or target schema.");
            }

            var diffResult = SchemaDiffEngine.Diff(sourceSchema, targetSchema, targetSession.DdlGenerator);

            if (diffResult.HasDifferences)
            {
                if (_aiAdvisor.IsEnabled && _options.Ai.Enabled)
                {
                    await MigrationAiAugmentor.EnrichWithAiAsync(diffResult.MigrationPlan, _aiAdvisor, cancellationToken)
                        .ConfigureAwait(false);
                }

                RenderDiffSummary(diffResult);

                if (diffResult.MigrationPlan.HasSteps)
                {
                    var apply = AnsiConsole.Confirm(
                        $"Apply [yellow]{diffResult.MigrationPlan.Steps.Count}[/] migration steps to target database [green]{targetSession.DatabaseName}[/]?",
                        defaultValue: false);

                    if (apply)
                    {
                        await _migrationExecutor.ApplyMigrationAsync(targetSession, diffResult.MigrationPlan, cancellationToken)
                            .ConfigureAwait(false);

                        AnsiConsole.MarkupLine("[green]Migrations applied successfully.[/]");
                    }
                }
            }
            else
            {
                AnsiConsole.MarkupLine("[green]No schema differences detected between source and target.[/]");
            }

            // Sync
            var runSync = AnsiConsole.Confirm("Run data sync now?", defaultValue: true);
            if (runSync)
            {
                var modeSelection = AnsiConsole.Prompt(
                    new SelectionPrompt<string>()
                        .Title("Select [green]sync mode[/]:")
                        .AddChoices("Full (truncate + reload)", "Append (insert only)"));

                var mode = modeSelection.StartsWith("Full", StringComparison.OrdinalIgnoreCase)
                    ? SyncMode.Full
                    : SyncMode.Append;

                var defaultBatchSize = _options.DefaultBatchSize <= 0 ? 5000 : _options.DefaultBatchSize;
                var batchSize = AnsiConsole.Ask<int>($"Batch size (default {defaultBatchSize}):", defaultBatchSize);

                if (mode == SyncMode.Full)
                {
                    var confirmFull = AnsiConsole.Confirm(
                        "Full mode will [red]truncate all matching tables on the target[/] before loading. Continue?",
                        defaultValue: false);
                    if (!confirmFull)
                    {
                        AnsiConsole.MarkupLine("[yellow]Sync cancelled by user.[/]");
                        return 0;
                    }
                }

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

                await AnsiConsole.Status()
                    .StartAsync("Syncing data...", async _ =>
                    {
                        await _syncEngine.SyncAsync(
                                sourceSession,
                                targetSession,
                                sourceSchema!,
                                targetSchema!,
                                options,
                                progress,
                                cancellationToken)
                            .ConfigureAwait(false);
                    });

                AnsiConsole.MarkupLine("[green]Data sync completed.[/]");
            }

            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Wizard failed.");
            AnsiConsole.MarkupLine($"[red]Error:[/] {ex.Message}");
            return -1;
        }
    }

    private static ProviderKind PromptProvider(string role)
    {
        var choice = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title($"Select [green]{role} provider[/]:")
                .AddChoices("SqlServer", "Postgres"));

        return choice.Equals("SqlServer", StringComparison.OrdinalIgnoreCase)
            ? ProviderKind.SqlServer
            : ProviderKind.Postgres;
    }

    private static ConnectionInfo PromptConnectionInfo(string role, ProviderKind kind)
    {
        AnsiConsole.MarkupLine($"[yellow]{role.ToUpperInvariant()} connection[/]");

        var host = AnsiConsole.Ask<string>("Host (e.g. localhost):", "localhost");
        int? port = null;

        if (kind == ProviderKind.Postgres)
        {
            var portText = AnsiConsole.Ask<string>("Port (blank for default 5432):", string.Empty);
            if (!string.IsNullOrWhiteSpace(portText) && int.TryParse(portText, out var parsedPort))
            {
                port = parsedPort;
            }
        }

        if (kind == ProviderKind.SqlServer)
        {
            var portText = AnsiConsole.Ask<string>("Port (blank for default 1433):", string.Empty);
            if (!string.IsNullOrWhiteSpace(portText) && int.TryParse(portText, out var parsedPort))
            {
                port = parsedPort;
            }
        }

        var username = AnsiConsole.Ask<string>("Username:", "sa");
        var password = AnsiConsole.Prompt(
            new TextPrompt<string>("Password:")
                .PromptStyle("grey")
                .Secret());

        var useSsl = kind == ProviderKind.Postgres &&
                     AnsiConsole.Confirm("Use SSL for this connection?", defaultValue: true);

        return new ConnectionInfo(kind, host, port, username, password, UseIntegratedSecurity: false, UseSsl: useSsl);
    }

    private static async Task<string> PromptDatabaseAsync(
        string role,
        IProvider provider,
        ConnectionInfo connection,
        CancellationToken cancellationToken)
    {
        AnsiConsole.MarkupLine($"Listing databases for [yellow]{role}[/]...");

        var databases = await provider.ListDatabasesAsync(connection, cancellationToken).ConfigureAwait(false);
        if (databases.Count == 0)
        {
            throw new InvalidOperationException("No databases were discovered on the server.");
        }

        var selected = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title($"Select [green]{role} database[/]:")
                .AddChoices(databases));

        return selected;
    }

    internal static void RenderDiffSummary(SchemaDiffResult diff)
    {
        var summaryTable = new Table()
            .Border(TableBorder.Rounded)
            .Title("[yellow]Schema diff summary[/]");

        summaryTable.AddColumn("Category");
        summaryTable.AddColumn("Count");

        var missingTables = 0;
        var extraTables = 0;
        var missingColumns = 0;
        var extraColumns = 0;
        var mismatchedColumns = 0;

        foreach (var t in diff.TableDifferences)
        {
            if (t.IsMissingInTarget) missingTables++;
            if (t.IsExtraInTarget) extraTables++;

            missingColumns += t.MissingColumns.Count;
            extraColumns += t.ExtraColumns.Count;
            mismatchedColumns += t.MismatchedColumns.Count;
        }

        summaryTable.AddRow("Missing tables in target", missingTables.ToString());
        summaryTable.AddRow("Extra tables in target", extraTables.ToString());
        summaryTable.AddRow("Missing columns in target", missingColumns.ToString());
        summaryTable.AddRow("Extra columns in target", extraColumns.ToString());
        summaryTable.AddRow("Mismatched columns", mismatchedColumns.ToString());

        AnsiConsole.Write(summaryTable);
        AnsiConsole.WriteLine();

        if (diff.MigrationPlan.HasSteps)
        {
            var planTable = new Table()
                .Border(TableBorder.Rounded)
                .Title("[yellow]Migration plan[/]");

            planTable.AddColumn("#");
            planTable.AddColumn("Kind");
            planTable.AddColumn("Table");
            planTable.AddColumn("Column");
            planTable.AddColumn("Risk");

            for (var i = 0; i < diff.MigrationPlan.Steps.Count; i++)
            {
                var step = diff.MigrationPlan.Steps[i];
                planTable.AddRow(
                    (i + 1).ToString(),
                    step.Kind.ToString(),
                    $"{step.SchemaName}.{step.TableName}",
                    step.Column?.Name ?? "-",
                    step.Risk.ToString());
            }

            AnsiConsole.Write(planTable);
            AnsiConsole.WriteLine();
        }
    }
}
