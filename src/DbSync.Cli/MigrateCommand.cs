using System;
using System.Threading;
using System.Threading.Tasks;
using DbSync.AI;
using DbSync.Core.Diff;
using DbSync.Engine;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Spectre.Console;
using Spectre.Console.Cli;

namespace DbSync.Cli;

public sealed class MigrateCommandSettings : ConnectionSettings
{
    [CommandOption("--apply")]
    public bool Apply { get; set; }
}

public sealed class MigrateCommand : AsyncCommand<MigrateCommandSettings>
{
    private readonly ProviderRegistry _providers;
    private readonly IMigrationExecutor _migrationExecutor;
    private readonly IAiAdvisor _aiAdvisor;
    private readonly DbSyncOptions _options;
    private readonly ILogger<MigrateCommand> _logger;

    public MigrateCommand(
        ProviderRegistry providers,
        IMigrationExecutor migrationExecutor,
        IAiAdvisor aiAdvisor,
        IOptions<DbSyncOptions> options,
        ILogger<MigrateCommand> logger)
    {
        _providers = providers ?? throw new ArgumentNullException(nameof(providers));
        _migrationExecutor = migrationExecutor ?? throw new ArgumentNullException(nameof(migrationExecutor));
        _aiAdvisor = aiAdvisor ?? throw new ArgumentNullException(nameof(aiAdvisor));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public override async Task<int> ExecuteAsync(CommandContext context, MigrateCommandSettings settings)
    {
        var cancellationToken = CancellationToken.None;

        try
        {
            var sourceKind = DiffCommand.ParseProviderKind(settings.SourceProvider);
            var targetKind = DiffCommand.ParseProviderKind(settings.TargetProvider);

            if (string.IsNullOrWhiteSpace(settings.SourceDatabase) ||
                string.IsNullOrWhiteSpace(settings.TargetDatabase))
            {
                throw new InvalidOperationException("Both --source-database and --target-database must be provided for migrate.");
            }

            var sourceProvider = _providers.GetByKind(sourceKind);
            var targetProvider = _providers.GetByKind(targetKind);

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

            var diffResult = SchemaDiffEngine.Diff(sourceSchema, targetSchema, targetSession.DdlGenerator);

            if (!diffResult.HasDifferences)
            {
                AnsiConsole.MarkupLine("[green]No schema differences detected; nothing to migrate.[/]");
                return 0;
            }

            if (_aiAdvisor.IsEnabled && _options.Ai.Enabled)
            {
                await MigrationAiAugmentor.EnrichWithAiAsync(diffResult.MigrationPlan, _aiAdvisor, cancellationToken)
                    .ConfigureAwait(false);
            }

            WizardCommand.RenderDiffSummary(diffResult);

            var shouldApply = settings.Apply ||
                              AnsiConsole.Confirm(
                                  $"Apply [yellow]{diffResult.MigrationPlan.Steps.Count}[/] migration steps to target database [green]{targetSession.DatabaseName}[/]?",
                                  defaultValue: false);

            if (!shouldApply)
            {
                AnsiConsole.MarkupLine("[yellow]Migration cancelled by user.[/]");
                return 0;
            }

            await _migrationExecutor.ApplyMigrationAsync(targetSession, diffResult.MigrationPlan, cancellationToken)
                .ConfigureAwait(false);

            AnsiConsole.MarkupLine("[green]Migrations applied successfully.[/]");
            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Migrate command failed.");
            AnsiConsole.MarkupLine($"[red]Error:[/] {ex.Message}");
            return -1;
        }
    }
}
