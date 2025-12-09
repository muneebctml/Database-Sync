using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DbSync.Core.Providers;
using DbSync.Core.Schema;
using Microsoft.Data.SqlClient;
using DataTable = System.Data.DataTable;
using CommandBehavior = System.Data.CommandBehavior;

namespace DbSync.Providers.SqlServer;

public sealed class SqlServerProvider : IProvider
{
    public ProviderKind Kind => ProviderKind.SqlServer;

    public string Name => "sqlserver";

    public async Task<bool> TestConnectionAsync(ConnectionInfo connection, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var sqlConnection = new SqlConnection(BuildConnectionString(connection, "master"));
            await sqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<string>> ListDatabasesAsync(ConnectionInfo connection, CancellationToken cancellationToken = default)
    {
        var databases = new List<string>();

        await using var sqlConnection = new SqlConnection(BuildConnectionString(connection, "master"));
        await sqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);

        const string sql = @"SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name;";

        await using var command = new SqlCommand(sql, sqlConnection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            databases.Add(reader.GetString(0));
        }

        return databases;
    }

    public async Task<IDbSession> ConnectAsync(ConnectionInfo connection, string databaseName, CancellationToken cancellationToken = default)
    {
        if (databaseName is null) throw new ArgumentNullException(nameof(databaseName));

        var sqlConnection = new SqlConnection(BuildConnectionString(connection, databaseName));
        await sqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);

        return new SqlServerDbSession(sqlConnection, databaseName);
    }

    private static string BuildConnectionString(ConnectionInfo connection, string databaseName)
    {
        if (connection is null) throw new ArgumentNullException(nameof(connection));
        if (string.IsNullOrWhiteSpace(connection.Host)) throw new ArgumentException("Host is required.", nameof(connection));

        var builder = new SqlConnectionStringBuilder
        {
            InitialCatalog = databaseName
        };

        builder.DataSource = connection.Port.HasValue
            ? $"{connection.Host},{connection.Port.Value}"
            : connection.Host;

        if (connection.UseIntegratedSecurity)
        {
            builder.IntegratedSecurity = true;
        }
        else
        {
            builder.UserID = connection.Username;
            builder.Password = connection.Password;
        }

        builder.TrustServerCertificate = true;
        builder.Encrypt = false;

        return builder.ConnectionString;
    }
}

internal sealed class SqlServerDbSession : IDbSession
{
    private readonly SqlConnection _connection;

    public SqlServerDbSession(SqlConnection connection, string databaseName)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));

        Introspector = new SqlServerIntrospector(_connection);
        DdlGenerator = new SqlServerDdlGenerator();
        DataReader = new SqlServerDataReader(_connection);
        DataWriter = new SqlServerDataWriter(_connection);
        Capabilities = new SqlServerCapabilities();
    }

    public string DatabaseName { get; }

    public IIntrospector Introspector { get; }

    public IDdlGenerator DdlGenerator { get; }

    public IDataReader DataReader { get; }

    public IDataWriter DataWriter { get; }

    public ICapabilities Capabilities { get; }

    public System.Data.Common.DbConnection Connection => _connection;

    public ValueTask DisposeAsync()
    {
        return _connection.DisposeAsync();
    }
}

internal sealed class SqlServerCapabilities : ICapabilities
{
    public bool SupportsTransactions => true;

    public bool SupportsTruncate => true;

    public bool SupportsBulkInsert => true;

    public bool SupportsUpsert => false;
}

internal sealed class SqlServerIntrospector : IIntrospector
{
    private readonly SqlConnection _connection;

    public SqlServerIntrospector(SqlConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public async Task<DatabaseSchema> GetDatabaseSchemaAsync(CancellationToken cancellationToken = default)
    {
        var tables = new Dictionary<(string Schema, string Name), List<ColumnSchema>>();

        const string columnsSql = @"
SELECT c.TABLE_SCHEMA,
       c.TABLE_NAME,
       c.COLUMN_NAME,
       c.IS_NULLABLE,
       c.DATA_TYPE,
       c.CHARACTER_MAXIMUM_LENGTH,
       c.NUMERIC_PRECISION,
       c.NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS c
JOIN INFORMATION_SCHEMA.TABLES t
  ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
 AND c.TABLE_NAME = t.TABLE_NAME
WHERE t.TABLE_TYPE = 'BASE TABLE'
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;";

        await using (var command = new SqlCommand(columnsSql, _connection))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var schema = reader.GetString(0);
                var table = reader.GetString(1);
                var columnName = reader.GetString(2);
                var isNullableString = reader.GetString(3);
                var dataType = reader.GetString(4);

                int? length = reader.IsDBNull(5) ? null : reader.GetInt32(5);
                int? precision = reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6));
                int? scale = reader.IsDBNull(7) ? null : Convert.ToInt32(reader.GetValue(7));

                var key = (schema, table);
                if (!tables.TryGetValue(key, out var columnList))
                {
                    columnList = new List<ColumnSchema>();
                    tables[key] = columnList;
                }

                var canonicalType = SqlServerTypeMapper.MapToCanonical(dataType);
                var isNullable = string.Equals(isNullableString, "YES", StringComparison.OrdinalIgnoreCase);

                columnList.Add(new ColumnSchema(columnName, canonicalType, dataType, isNullable, length, precision, scale));
            }
        }

        // Primary keys
        var primaryKeys = new Dictionary<(string Schema, string Name), List<string>>();

        const string pkSql = @"
SELECT KU.TABLE_SCHEMA,
       KU.TABLE_NAME,
       KU.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
  ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
ORDER BY KU.TABLE_SCHEMA, KU.TABLE_NAME, KU.ORDINAL_POSITION;";

        await using (var command = new SqlCommand(pkSql, _connection))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var schema = reader.GetString(0);
                var table = reader.GetString(1);
                var columnName = reader.GetString(2);

                var key = (schema, table);
                if (!primaryKeys.TryGetValue(key, out var columns))
                {
                    columns = new List<string>();
                    primaryKeys[key] = columns;
                }

                columns.Add(columnName);
            }
        }

        var tableSchemas = new List<TableSchema>();
        foreach (var kvp in tables)
        {
            primaryKeys.TryGetValue(kvp.Key, out var pkColumns);
            var pk = pkColumns is { Count: > 0 } ? new PrimaryKeySchema(pkColumns) : null;

            tableSchemas.Add(new TableSchema(kvp.Key.Schema, kvp.Key.Name, kvp.Value, pk));
        }

        var databaseName = _connection.Database;
        return new DatabaseSchema(databaseName, tableSchemas);
    }
}

internal static class SqlServerTypeMapper
{
    public static CanonicalDataType MapToCanonical(string dataType)
    {
        if (dataType is null) throw new ArgumentNullException(nameof(dataType));

        return dataType.ToLowerInvariant() switch
        {
            "varchar" or "nvarchar" or "char" or "nchar" or "text" or "ntext" => CanonicalDataType.String,
            "int" or "smallint" or "tinyint" => CanonicalDataType.Int32,
            "bigint" => CanonicalDataType.Int64,
            "decimal" or "numeric" or "money" or "smallmoney" => CanonicalDataType.Decimal,
            "float" or "real" => CanonicalDataType.Double,
            "bit" => CanonicalDataType.Boolean,
            "datetime" or "smalldatetime" or "datetime2" or "date" or "time" => CanonicalDataType.DateTime,
            "datetimeoffset" => CanonicalDataType.DateTimeOffset,
            "uniqueidentifier" => CanonicalDataType.Guid,
            "varbinary" or "binary" or "image" => CanonicalDataType.Binary,
            _ => CanonicalDataType.String
        };
    }

    public static string ToSqlType(ColumnSchema column)
    {
        return column.Type switch
        {
            CanonicalDataType.String => BuildStringType(column),
            CanonicalDataType.Int32 => "INT",
            CanonicalDataType.Int64 => "BIGINT",
            CanonicalDataType.Decimal => BuildDecimalType(column),
            CanonicalDataType.Double => "FLOAT",
            CanonicalDataType.Boolean => "BIT",
            CanonicalDataType.DateTime => "DATETIME2",
            CanonicalDataType.DateTimeOffset => "DATETIMEOFFSET",
            CanonicalDataType.Guid => "UNIQUEIDENTIFIER",
            CanonicalDataType.Json => "NVARCHAR(MAX)",
            CanonicalDataType.Binary => "VARBINARY(MAX)",
            _ => "NVARCHAR(MAX)"
        };
    }

    private static string BuildStringType(ColumnSchema column)
    {
        if (column.Length is { } length && length > 0 && length <= 4000)
        {
            return $"NVARCHAR({length})";
        }

        return "NVARCHAR(MAX)";
    }

    private static string BuildDecimalType(ColumnSchema column)
    {
        var precision = column.Precision is > 0 ? column.Precision.Value : 18;
        var scale = column.Scale is > 0 ? column.Scale.Value : 2;
        return $"DECIMAL({precision},{scale})";
    }
}

internal sealed class SqlServerDdlGenerator : IDdlGenerator
{
    public string GenerateCreateTable(TableSchema table)
    {
        if (table is null) throw new ArgumentNullException(nameof(table));

        var builder = new StringBuilder();
        builder.Append("CREATE TABLE ");
        builder.Append(QuoteIdentifier(table.SchemaName));
        builder.Append('.');
        builder.Append(QuoteIdentifier(table.TableName));
        builder.AppendLine(" (");

        for (var i = 0; i < table.Columns.Count; i++)
        {
            var column = table.Columns[i];
            builder.Append("    ");
            builder.Append(QuoteIdentifier(column.Name));
            builder.Append(' ');
            builder.Append(SqlServerTypeMapper.ToSqlType(column));
            builder.Append(column.IsNullable ? " NULL" : " NOT NULL");

            if (i < table.Columns.Count - 1 || table.PrimaryKey is not null)
            {
                builder.Append(',');
            }

            builder.AppendLine();
        }

        if (table.PrimaryKey is not null)
        {
            builder.Append("    CONSTRAINT ");
            builder.Append(QuoteIdentifier($"PK_{table.SchemaName}_{table.TableName}"));
            builder.Append(" PRIMARY KEY (");

            for (var i = 0; i < table.PrimaryKey.Columns.Count; i++)
            {
                if (i > 0)
                {
                    builder.Append(", ");
                }

                builder.Append(QuoteIdentifier(table.PrimaryKey.Columns[i]));
            }

            builder.AppendLine(")");
        }

        builder.Append(");");
        return builder.ToString();
    }

    public string GenerateAddColumn(TableSchema table, ColumnSchema column)
    {
        if (table is null) throw new ArgumentNullException(nameof(table));
        if (column is null) throw new ArgumentNullException(nameof(column));

        var builder = new StringBuilder();
        builder.Append("ALTER TABLE ");
        builder.Append(QuoteIdentifier(table.SchemaName));
        builder.Append('.');
        builder.Append(QuoteIdentifier(table.TableName));
        builder.Append(" ADD ");
        builder.Append(QuoteIdentifier(column.Name));
        builder.Append(' ');
        builder.Append(SqlServerTypeMapper.ToSqlType(column));
        builder.Append(column.IsNullable ? " NULL;" : " NOT NULL;");
        return builder.ToString();
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";
    }
}

internal sealed class SqlServerDataReader : IDataReader
{
    private readonly SqlConnection _connection;

    public SqlServerDataReader(SqlConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public async IAsyncEnumerable<RowData> ReadTableAsync(TableSchema table, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (table is null) throw new ArgumentNullException(nameof(table));

        var columnNames = table.Columns.Select(c => c.Name).ToArray();
        var selectList = string.Join(", ", columnNames.Select(QuoteIdentifier));
        var sql = $"SELECT {selectList} FROM {QuoteIdentifier(table.SchemaName)}.{QuoteIdentifier(table.TableName)}";

        await using var command = new SqlCommand(sql, _connection);
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var values = new Dictionary<string, object?>(columnNames.Length, StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < columnNames.Length; i++)
            {
                var isNull = await reader.IsDBNullAsync(i, cancellationToken).ConfigureAwait(false);
                values[columnNames[i]] = isNull ? null : reader.GetValue(i);
            }

            yield return new RowData(values);
        }
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";
    }
}

internal sealed class SqlServerDataWriter : IDataWriter
{
    private readonly SqlConnection _connection;

    public SqlServerDataWriter(SqlConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public async Task TruncateTableAsync(TableSchema table, CancellationToken cancellationToken = default)
    {
        if (table is null) throw new ArgumentNullException(nameof(table));

        var sql = $"TRUNCATE TABLE {QuoteIdentifier(table.SchemaName)}.{QuoteIdentifier(table.TableName)}";
        await ExecuteCommandAsync(sql, cancellationToken).ConfigureAwait(false);
    }

    public async Task InsertBatchAsync(TableSchema table, IReadOnlyList<RowData> rows, CancellationToken cancellationToken = default)
    {
        if (table is null) throw new ArgumentNullException(nameof(table));
        if (rows is null) throw new ArgumentNullException(nameof(rows));
        if (rows.Count == 0)
        {
            return;
        }

        var dataTable = new DataTable();
        foreach (var column in table.Columns)
        {
            dataTable.Columns.Add(column.Name, typeof(object));
        }

        foreach (var row in rows)
        {
            var values = new object?[table.Columns.Count];
            for (var i = 0; i < table.Columns.Count; i++)
            {
                var columnName = table.Columns[i].Name;
                values[i] = row.GetValue(columnName) ?? DBNull.Value;
            }

            dataTable.Rows.Add(values);
        }

        using var bulkCopy = new SqlBulkCopy(_connection)
        {
            DestinationTableName = $"{QuoteIdentifier(table.SchemaName)}.{QuoteIdentifier(table.TableName)}",
            BatchSize = rows.Count
        };

        foreach (var column in table.Columns)
        {
            bulkCopy.ColumnMappings.Add(column.Name, column.Name);
        }

        await bulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
    }

    public async Task ExecuteCommandAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentException("SQL must not be empty.", nameof(sql));

        await using var command = new SqlCommand(sql, _connection);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";
    }
}
