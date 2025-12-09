using System;
using System.Threading;
using System.Threading.Tasks;
using DbSync.Core.Providers;
using DbSync.Core.Schema;
using Microsoft.Extensions.Logging;
using Spectre.Console;
using Spectre.Console.Cli;

namespace DbSync.Cli;

public sealed class TargetTruncateSettings : CommandSettings
{
    [CommandOption("--provider <PROVIDER>")]
    public string Provider { get; set; } = string.Empty;

    [CommandOption("--host <HOST>")]
    public string Host { get; set; } = "localhost";

    [CommandOption("--port <PORT>")]
    public int? Port { get; set; }

    [CommandOption("--username <USERNAME>")]
    public string Username { get; set; } = string.Empty;

    [CommandOption("--password <PASSWORD>")]
    public string Password { get; set; } = string.Empty;

    [CommandOption("--use-ssl")]
    public bool UseSsl { get; set; }

    [CommandOption("--database <DATABASE>")]
    public string Database { get; set; } = string.Empty;

    [CommandOption("--table <TABLE>")]
    public string Table { get; set; } = string.Empty;

    [CommandOption("--schema <SCHEMA>")]
    public string Schema { get; set; } = "public";
}

public sealed class TargetTruncateCommand : AsyncCommand<TargetTruncateSettings>
{
    private readonly ProviderRegistry _providers;
    private readonly ILogger<TargetTruncateCommand> _logger;

    public TargetTruncateCommand(ProviderRegistry providers, ILogger<TargetTruncateCommand> logger)
    {
        _providers = providers ?? throw new ArgumentNullException(nameof(providers));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public override async Task<int> ExecuteAsync(CommandContext context, TargetTruncateSettings settings)
    {
        var cancellationToken = CancellationToken.None;

        try
        {
            if (string.IsNullOrWhiteSpace(settings.Table))
            {
                throw new InvalidOperationException("--table is required.");
            }

            if (string.IsNullOrWhiteSpace(settings.Database))
            {
                throw new InvalidOperationException("--database is required.");
            }

            var kind = DiffCommand.ParseProviderKind(settings.Provider);
            var provider = _providers.GetByKind(kind);

            var connection = new ConnectionInfo(
                kind,
                settings.Host,
                settings.Port,
                settings.Username,
                settings.Password,
                UseIntegratedSecurity: false,
                UseSsl: settings.UseSsl);

            var confirm = AnsiConsole.Confirm(
                $"[red]Truncate[/] table [yellow]{settings.Schema}.{settings.Table}[/] on database [green]{settings.Database}[/]?",
                defaultValue: false);
            if (!confirm)
            {
                AnsiConsole.MarkupLine("[yellow]Operation cancelled by user.[/]");
                return 0;
            }

            await using var session = await provider.ConnectAsync(connection, settings.Database, cancellationToken)
                .ConfigureAwait(false);

            var table = new TableSchema(settings.Schema, settings.Table, Array.Empty<ColumnSchema>());
            await session.DataWriter.TruncateTableAsync(table, cancellationToken).ConfigureAwait(false);

            AnsiConsole.MarkupLine("[green]Table truncated successfully.[/]");
            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Target truncate failed.");
            AnsiConsole.MarkupLine($"[red]Error:[/] {ex.Message}");
            return -1;
        }
    }
}

public sealed class TargetDeleteSettings : CommandSettings
{
    [CommandOption("--provider <PROVIDER>")]
    public string Provider { get; set; } = string.Empty;

    [CommandOption("--host <HOST>")]
    public string Host { get; set; } = "localhost";

    [CommandOption("--port <PORT>")]
    public int? Port { get; set; }

    [CommandOption("--username <USERNAME>")]
    public string Username { get; set; } = string.Empty;

    [CommandOption("--password <PASSWORD>")]
    public string Password { get; set; } = string.Empty;

    [CommandOption("--use-ssl")]
    public bool UseSsl { get; set; }

    [CommandOption("--database <DATABASE>")]
    public string Database { get; set; } = string.Empty;

    [CommandOption("--table <TABLE>")]
    public string Table { get; set; } = string.Empty;

    [CommandOption("--schema <SCHEMA>")]
    public string Schema { get; set; } = "public";

    [CommandOption("--where <SQL>")]
    public string Where { get; set; } = string.Empty;
}

public sealed class TargetDeleteCommand : AsyncCommand<TargetDeleteSettings>
{
    private readonly ProviderRegistry _providers;
    private readonly ILogger<TargetDeleteCommand> _logger;

    public TargetDeleteCommand(ProviderRegistry providers, ILogger<TargetDeleteCommand> logger)
    {
        _providers = providers ?? throw new ArgumentNullException(nameof(providers));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public override async Task<int> ExecuteAsync(CommandContext context, TargetDeleteSettings settings)
    {
        var cancellationToken = CancellationToken.None;

        try
        {
            if (string.IsNullOrWhiteSpace(settings.Table))
            {
                throw new InvalidOperationException("--table is required.");
            }

            if (string.IsNullOrWhiteSpace(settings.Database))
            {
                throw new InvalidOperationException("--database is required.");
            }

            if (string.IsNullOrWhiteSpace(settings.Where))
            {
                throw new InvalidOperationException("A non-empty --where clause is required for delete operations.");
            }

            var kind = DiffCommand.ParseProviderKind(settings.Provider);
            var provider = _providers.GetByKind(kind);

            var connection = new ConnectionInfo(
                kind,
                settings.Host,
                settings.Port,
                settings.Username,
                settings.Password,
                UseIntegratedSecurity: false,
                UseSsl: settings.UseSsl);

            var confirm = AnsiConsole.Confirm(
                $"Delete rows from [yellow]{settings.Schema}.{settings.Table}[/] on database [green]{settings.Database}[/] with WHERE [grey]{settings.Where}[/]?",
                defaultValue: false);
            if (!confirm)
            {
                AnsiConsole.MarkupLine("[yellow]Operation cancelled by user.[/]");
                return 0;
            }

            await using var session = await provider.ConnectAsync(connection, settings.Database, cancellationToken)
                .ConfigureAwait(false);

            var qualifiedTable = $"{settings.Schema}.{settings.Table}";
            var sql = $"DELETE FROM {qualifiedTable} WHERE {settings.Where}";

            await session.DataWriter.ExecuteCommandAsync(sql, cancellationToken).ConfigureAwait(false);

            AnsiConsole.MarkupLine("[green]Delete command executed.[/]");
            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Target delete failed.");
            AnsiConsole.MarkupLine($"[red]Error:[/] {ex.Message}");
            return -1;
        }
    }
}

public sealed class TargetUpdateSettings : CommandSettings
{
    [CommandOption("--provider <PROVIDER>")]
    public string Provider { get; set; } = string.Empty;

    [CommandOption("--host <HOST>")]
    public string Host { get; set; } = "localhost";

    [CommandOption("--port <PORT>")]
    public int? Port { get; set; }

    [CommandOption("--username <USERNAME>")]
    public string Username { get; set; } = string.Empty;

    [CommandOption("--password <PASSWORD>")]
    public string Password { get; set; } = string.Empty;

    [CommandOption("--use-ssl")]
    public bool UseSsl { get; set; }

    [CommandOption("--database <DATABASE>")]
    public string Database { get; set; } = string.Empty;

    [CommandOption("--table <TABLE>")]
    public string Table { get; set; } = string.Empty;

    [CommandOption("--schema <SCHEMA>")]
    public string Schema { get; set; } = "public";

    [CommandOption("--set <SQL>")]
    public string Set { get; set; } = string.Empty;

    [CommandOption("--where <SQL>")]
    public string Where { get; set; } = string.Empty;
}

public sealed class TargetUpdateCommand : AsyncCommand<TargetUpdateSettings>
{
    private readonly ProviderRegistry _providers;
    private readonly ILogger<TargetUpdateCommand> _logger;

    public TargetUpdateCommand(ProviderRegistry providers, ILogger<TargetUpdateCommand> logger)
    {
        _providers = providers ?? throw new ArgumentNullException(nameof(providers));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public override async Task<int> ExecuteAsync(CommandContext context, TargetUpdateSettings settings)
    {
        var cancellationToken = CancellationToken.None;

        try
        {
            if (string.IsNullOrWhiteSpace(settings.Table))
            {
                throw new InvalidOperationException("--table is required.");
            }

            if (string.IsNullOrWhiteSpace(settings.Database))
            {
                throw new InvalidOperationException("--database is required.");
            }

            if (string.IsNullOrWhiteSpace(settings.Set))
            {
                throw new InvalidOperationException("--set clause is required for update operations.");
            }

            if (string.IsNullOrWhiteSpace(settings.Where))
            {
                throw new InvalidOperationException("A non-empty --where clause is required for update operations.");
            }

            var kind = DiffCommand.ParseProviderKind(settings.Provider);
            var provider = _providers.GetByKind(kind);

            var connection = new ConnectionInfo(
                kind,
                settings.Host,
                settings.Port,
                settings.Username,
                settings.Password,
                UseIntegratedSecurity: false,
                UseSsl: settings.UseSsl);

            var confirm = AnsiConsole.Confirm(
                $"Update rows in [yellow]{settings.Schema}.{settings.Table}[/] on database [green]{settings.Database}[/] with SET [grey]{settings.Set}[/] WHERE [grey]{settings.Where}[/]?",
                defaultValue: false);
            if (!confirm)
            {
                AnsiConsole.MarkupLine("[yellow]Operation cancelled by user.[/]");
                return 0;
            }

            await using var session = await provider.ConnectAsync(connection, settings.Database, cancellationToken)
                .ConfigureAwait(false);

            var qualifiedTable = $"{settings.Schema}.{settings.Table}";
            var sql = $"UPDATE {qualifiedTable} SET {settings.Set} WHERE {settings.Where}";

            await session.DataWriter.ExecuteCommandAsync(sql, cancellationToken).ConfigureAwait(false);

            AnsiConsole.MarkupLine("[green]Update command executed.[/]");
            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Target update failed.");
            AnsiConsole.MarkupLine($"[red]Error:[/] {ex.Message}");
            return -1;
        }
    }
}
