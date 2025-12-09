using System;
using System.Threading.Tasks;
using DbSync.AI;
using DbSync.Core.Providers;
using DbSync.Engine;
using DbSync.Providers.Postgres;
using DbSync.Providers.SqlServer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Spectre.Console.Cli;

namespace DbSync.Cli;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var hostBuilder = Host.CreateApplicationBuilder(args);

        hostBuilder.Configuration
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables("DBSYNC_");

        hostBuilder.Services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.AddSimpleConsole();
        });

        hostBuilder.Services.Configure<DbSyncOptions>(
            hostBuilder.Configuration.GetSection(DbSyncOptions.ConfigurationSectionName));

        // Core services
        hostBuilder.Services.AddSingleton<ISyncEngine, DatabaseSyncEngine>();
        hostBuilder.Services.AddSingleton<IMigrationExecutor, MigrationExecutor>();

        // Providers
        hostBuilder.Services.AddSingleton<IProvider, SqlServerProvider>();
        hostBuilder.Services.AddSingleton<IProvider, PostgresProvider>();
        hostBuilder.Services.AddSingleton<ProviderRegistry>();

        // AI advisor
        var aiMode = hostBuilder.Configuration["DbSync:Ai:Mode"] ?? "None";
        if (string.Equals(aiMode, "Mcp", StringComparison.OrdinalIgnoreCase))
        {
            hostBuilder.Services.AddSingleton<IAiAdvisor, McpAiAdvisor>();
        }
        else
        {
            hostBuilder.Services.AddSingleton<IAiAdvisor, NullAiAdvisor>();
        }

        // Commands
        hostBuilder.Services.AddSingleton<WizardCommand>();
        hostBuilder.Services.AddSingleton<DiffCommand>();
        hostBuilder.Services.AddSingleton<SyncCommand>();
        hostBuilder.Services.AddSingleton<MigrateCommand>();
        hostBuilder.Services.AddSingleton<TargetTruncateCommand>();
        hostBuilder.Services.AddSingleton<TargetDeleteCommand>();
        hostBuilder.Services.AddSingleton<TargetUpdateCommand>();

        var registrar = new TypeRegistrar(hostBuilder.Services);
        var app = new CommandApp(registrar);

        app.Configure(config =>
        {
            config.SetApplicationName("dbsync");

            config.AddCommand<WizardCommand>("wizard")
                .WithDescription("Interactive end-to-end wizard for diff, migrate, and sync.");

            config.AddCommand<DiffCommand>("diff")
                .WithDescription("Compare schemas between source and target databases.");

            config.AddCommand<MigrateCommand>("migrate")
                .WithDescription("Apply schema migrations to target based on a diff.");

            config.AddCommand<SyncCommand>("sync")
                .WithDescription("Synchronize data from source to target.");

            config.AddBranch("target", target =>
            {
                target.AddCommand<TargetTruncateCommand>("truncate")
                    .WithDescription("Truncate a table on the target database.");

                target.AddCommand<TargetDeleteCommand>("delete")
                    .WithDescription("Delete rows from a target table using a WHERE clause.");

                target.AddCommand<TargetUpdateCommand>("update")
                    .WithDescription("Update rows in a target table using SET and WHERE clauses.");
            });
        });

        try
        {
            return await app.RunAsync(args).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex);
            return -1;
        }
    }
}

