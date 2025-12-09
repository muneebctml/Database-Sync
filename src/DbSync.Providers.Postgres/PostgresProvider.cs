using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DbSync.Core.Providers;
using DbSync.Core.Schema;
using Npgsql;

namespace DbSync.Providers.Postgres;

public sealed class PostgresProvider : IProvider
{
    public ProviderKind Kind => ProviderKind.Postgres;

    public string Name => "postgres";

    public async Task<bool> TestConnectionAsync(ConnectionInfo connection, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var npgsqlConnection = new NpgsqlConnection(BuildConnectionString(connection, "postgres"));
            await npgsqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
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

        await using var npgsqlConnection = new NpgsqlConnection(BuildConnectionString(connection, "postgres"));
        await npgsqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);

        const string sql = @"SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;";

        await using var command = new NpgsqlCommand(sql, npgsqlConnection);
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

        var npgsqlConnection = new NpgsqlConnection(BuildConnectionString(connection, databaseName));
        await npgsqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);

        return new PostgresDbSession(npgsqlConnection, databaseName);
    }

    private static string BuildConnectionString(ConnectionInfo connection, string databaseName)
    {
        if (connection is null) throw new ArgumentNullException(nameof(connection));
        if (string.IsNullOrWhiteSpace(connection.Host)) throw new ArgumentException("Host is required.", nameof(connection));

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = connection.Host,
            Database = databaseName,
            Username = connection.Username,
            Password = connection.Password,
            SslMode = connection.UseSsl ? SslMode.Require : SslMode.Disable,
            TrustServerCertificate = true
        };

        if (connection.Port.HasValue)
        {
            builder.Port = connection.Port.Value;
        }

        return builder.ConnectionString;
    }
}

internal sealed class PostgresDbSession : IDbSession
{
    private readonly NpgsqlConnection _connection;

    public PostgresDbSession(NpgsqlConnection connection, string databaseName)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));

        Introspector = new PostgresIntrospector(_connection);
        DdlGenerator = new PostgresDdlGenerator();
        DataReader = new PostgresDataReader(_connection);
        DataWriter = new PostgresDataWriter(_connection);
        Capabilities = new PostgresCapabilities();
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

internal sealed class PostgresCapabilities : ICapabilities
{
    public bool SupportsTransactions => true;

    public bool SupportsTruncate => true;

    public bool SupportsBulkInsert => true;

    public bool SupportsUpsert => false;
}

internal sealed class PostgresIntrospector : IIntrospector
{
    private readonly NpgsqlConnection _connection;

    public PostgresIntrospector(NpgsqlConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public async Task<DatabaseSchema> GetDatabaseSchemaAsync(CancellationToken cancellationToken = default)
    {
        var tables = new Dictionary<(string Schema, string Name), List<ColumnSchema>>();

        const string columnsSql = @"
SELECT table_schema,
       table_name,
       column_name,
       is_nullable,
       data_type,
       character_maximum_length,
       numeric_precision,
       numeric_scale
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name, ordinal_position;";

        await using (var command = new NpgsqlCommand(columnsSql, _connection))
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

                var canonicalType = PostgresTypeMapper.MapToCanonical(dataType);
                var isNullable = string.Equals(isNullableString, "YES", StringComparison.OrdinalIgnoreCase);

                columnList.Add(new ColumnSchema(columnName, canonicalType, dataType, isNullable, length, precision, scale));
            }
        }

        // Primary keys
        var primaryKeys = new Dictionary<(string Schema, string Name), List<string>>();

        const string pkSql = @"
SELECT
    tc.table_schema,
    tc.table_name,
    kc.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kc
  ON kc.constraint_name = tc.constraint_name
 AND kc.table_schema = tc.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
ORDER BY tc.table_schema, tc.table_name, kc.ordinal_position;";

        await using (var command = new NpgsqlCommand(pkSql, _connection))
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

internal static class PostgresTypeMapper
{
    public static CanonicalDataType MapToCanonical(string dataType)
    {
        if (dataType is null) throw new ArgumentNullException(nameof(dataType));

        return dataType.ToLowerInvariant() switch
        {
            "character varying" or "varchar" or "character" or "char" or "text" => CanonicalDataType.String,
            "integer" or "int" or "smallint" => CanonicalDataType.Int32,
            "bigint" => CanonicalDataType.Int64,
            "numeric" or "money" => CanonicalDataType.Decimal,
            "double precision" or "real" => CanonicalDataType.Double,
            "boolean" => CanonicalDataType.Boolean,
            "timestamp without time zone" or "timestamp" or "date" or "time without time zone" => CanonicalDataType.DateTime,
            "timestamp with time zone" or "time with time zone" => CanonicalDataType.DateTimeOffset,
            "uuid" => CanonicalDataType.Guid,
            "json" or "jsonb" => CanonicalDataType.Json,
            "bytea" => CanonicalDataType.Binary,
            _ => CanonicalDataType.String
        };
    }

    public static string ToSqlType(ColumnSchema column)
    {
        return column.Type switch
        {
            CanonicalDataType.String => BuildStringType(column),
            CanonicalDataType.Int32 => "integer",
            CanonicalDataType.Int64 => "bigint",
            CanonicalDataType.Decimal => BuildNumericType(column),
            CanonicalDataType.Double => "double precision",
            CanonicalDataType.Boolean => "boolean",
            CanonicalDataType.DateTime => "timestamp without time zone",
            CanonicalDataType.DateTimeOffset => "timestamp with time zone",
            CanonicalDataType.Guid => "uuid",
            CanonicalDataType.Json => "jsonb",
            CanonicalDataType.Binary => "bytea",
            _ => "text"
        };
    }

    private static string BuildStringType(ColumnSchema column)
    {
        if (column.Length is { } length && length > 0 && length <= 10485760)
        {
            return $"varchar({length})";
        }

        return "text";
    }

    private static string BuildNumericType(ColumnSchema column)
    {
        var precision = column.Precision is > 0 ? column.Precision.Value : 18;
        var scale = column.Scale is > 0 ? column.Scale.Value : 2;
        return $"numeric({precision},{scale})";
    }

    public static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
    }
}

internal sealed class PostgresDdlGenerator : IDdlGenerator
{
    public string GenerateCreateTable(TableSchema table)
    {
        if (table is null) throw new ArgumentNullException(nameof(table));

        var builder = new StringBuilder();
        builder.Append("CREATE TABLE ");
        builder.Append(PostgresTypeMapper.QuoteIdentifier(table.SchemaName));
        builder.Append('.');
        builder.Append(PostgresTypeMapper.QuoteIdentifier(table.TableName));
        builder.AppendLine(" (");

        for (var i = 0; i < table.Columns.Count; i++)
        {
            var column = table.Columns[i];
            builder.Append("    ");
            builder.Append(PostgresTypeMapper.QuoteIdentifier(column.Name));
            builder.Append(' ');
            builder.Append(PostgresTypeMapper.ToSqlType(column));
            builder.Append(column.IsNullable ? " NULL" : " NOT NULL");

            if (i < table.Columns.Count - 1 || table.PrimaryKey is not null)
            {
                builder.Append(',');
            }

            builder.AppendLine();
        }

        if (table.PrimaryKey is not null)
        {
            builder.Append("    PRIMARY KEY (");
            for (var i = 0; i < table.PrimaryKey.Columns.Count; i++)
            {
                if (i > 0)
                {
                    builder.Append(", ");
                }

                builder.Append(PostgresTypeMapper.QuoteIdentifier(table.PrimaryKey.Columns[i]));
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
        builder.Append(PostgresTypeMapper.QuoteIdentifier(table.SchemaName));
        builder.Append('.');
        builder.Append(PostgresTypeMapper.QuoteIdentifier(table.TableName));
        builder.Append(" ADD COLUMN ");
        builder.Append(PostgresTypeMapper.QuoteIdentifier(column.Name));
        builder.Append(' ');
        builder.Append(PostgresTypeMapper.ToSqlType(column));
        builder.Append(column.IsNullable ? " NULL;" : " NOT NULL;");
        return builder.ToString();
    }
}

internal sealed class PostgresDataReader : IDataReader
{
    private readonly NpgsqlConnection _connection;

    public PostgresDataReader(NpgsqlConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public async IAsyncEnumerable<RowData> ReadTableAsync(TableSchema table, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (table is null) throw new ArgumentNullException(nameof(table));

        var columnNames = table.Columns.Select(c => c.Name).ToArray();
        var selectList = string.Join(", ", columnNames.Select(PostgresTypeMapper.QuoteIdentifier));
        var sql = $"SELECT {selectList} FROM {PostgresTypeMapper.QuoteIdentifier(table.SchemaName)}.{PostgresTypeMapper.QuoteIdentifier(table.TableName)}";

        await using var command = new NpgsqlCommand(sql, _connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

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
}

internal sealed class PostgresDataWriter : IDataWriter
{
    private readonly NpgsqlConnection _connection;

    public PostgresDataWriter(NpgsqlConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public async Task TruncateTableAsync(TableSchema table, CancellationToken cancellationToken = default)
    {
        if (table is null) throw new ArgumentNullException(nameof(table));

        var sql = $"TRUNCATE TABLE {PostgresTypeMapper.QuoteIdentifier(table.SchemaName)}.{PostgresTypeMapper.QuoteIdentifier(table.TableName)}";
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

        var columnNames = table.Columns.Select(c => c.Name).ToArray();

        var sqlBuilder = new StringBuilder();
        sqlBuilder.Append("INSERT INTO ");
        sqlBuilder.Append(PostgresTypeMapper.QuoteIdentifier(table.SchemaName));
        sqlBuilder.Append('.');
        sqlBuilder.Append(PostgresTypeMapper.QuoteIdentifier(table.TableName));
        sqlBuilder.Append(" (");
        sqlBuilder.Append(string.Join(", ", columnNames.Select(PostgresTypeMapper.QuoteIdentifier)));
        sqlBuilder.Append(") VALUES ");

        await using var command = new NpgsqlCommand();
        command.Connection = _connection;

        for (var i = 0; i < rows.Count; i++)
        {
            if (i > 0)
            {
                sqlBuilder.Append(", ");
            }

            sqlBuilder.Append('(');

            for (var j = 0; j < columnNames.Length; j++)
            {
                if (j > 0)
                {
                    sqlBuilder.Append(", ");
                }

                var parameterName = $"p{i}_{j}";
                sqlBuilder.Append('@');
                sqlBuilder.Append(parameterName);

                var value = rows[i].GetValue(columnNames[j]) ?? DBNull.Value;
                command.Parameters.AddWithValue(parameterName, value);
            }

            sqlBuilder.Append(')');
        }

        command.CommandText = sqlBuilder.ToString();
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task ExecuteCommandAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentException("SQL must not be empty.", nameof(sql));

        await using var command = new NpgsqlCommand(sql, _connection);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
