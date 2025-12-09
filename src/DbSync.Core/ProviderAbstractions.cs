using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DbSync.Core.Schema;

namespace DbSync.Core.Providers;

public enum ProviderKind
{
    SqlServer,
    Postgres
}

public sealed record ConnectionInfo(
    ProviderKind Provider,
    string Host,
    int? Port,
    string Username,
    string Password,
    bool UseIntegratedSecurity = false,
    bool UseSsl = false);

public interface IProvider
{
    ProviderKind Kind { get; }

    string Name { get; }

    Task<bool> TestConnectionAsync(ConnectionInfo connection, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<string>> ListDatabasesAsync(ConnectionInfo connection, CancellationToken cancellationToken = default);

    Task<IDbSession> ConnectAsync(ConnectionInfo connection, string databaseName, CancellationToken cancellationToken = default);
}

public interface IDbSession : IAsyncDisposable
{
    string DatabaseName { get; }

    IIntrospector Introspector { get; }

    IDdlGenerator DdlGenerator { get; }

    IDataReader DataReader { get; }

    IDataWriter DataWriter { get; }

    ICapabilities Capabilities { get; }

    DbConnection Connection { get; }
}

public interface IIntrospector
{
    Task<DatabaseSchema> GetDatabaseSchemaAsync(CancellationToken cancellationToken = default);
}

public interface IDdlGenerator
{
    string GenerateCreateTable(TableSchema table);

    string GenerateAddColumn(TableSchema table, ColumnSchema column);
}

public interface IDataReader
{
    IAsyncEnumerable<RowData> ReadTableAsync(TableSchema table, CancellationToken cancellationToken = default);
}

public interface IDataWriter
{
    Task TruncateTableAsync(TableSchema table, CancellationToken cancellationToken = default);

    Task InsertBatchAsync(TableSchema table, IReadOnlyList<RowData> rows, CancellationToken cancellationToken = default);

    Task ExecuteCommandAsync(string sql, CancellationToken cancellationToken = default);
}

public interface ICapabilities
{
    bool SupportsTransactions { get; }

    bool SupportsTruncate { get; }

    bool SupportsBulkInsert { get; }

    bool SupportsUpsert { get; }
}

public sealed class RowData
{
    public RowData(IReadOnlyDictionary<string, object?> values)
    {
        Values = values ?? throw new ArgumentNullException(nameof(values));
    }

    public IReadOnlyDictionary<string, object?> Values { get; }

    public object? GetValue(string columnName)
    {
        if (columnName is null) throw new ArgumentNullException(nameof(columnName));

        return Values.TryGetValue(columnName, out var value) ? value : null;
    }
}

