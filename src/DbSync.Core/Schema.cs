using System;
using System.Collections.Generic;
using System.Linq;

namespace DbSync.Core.Schema;

public enum CanonicalDataType
{
    String,
    Int32,
    Int64,
    Decimal,
    Double,
    Boolean,
    DateTime,
    DateTimeOffset,
    Guid,
    Json,
    Binary
}

public sealed class DatabaseSchema
{
    public DatabaseSchema(string name, IReadOnlyList<TableSchema> tables)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Tables = tables ?? throw new ArgumentNullException(nameof(tables));
    }

    public string Name { get; }

    public IReadOnlyList<TableSchema> Tables { get; }

    public TableSchema? FindTable(string schemaName, string tableName)
    {
        if (schemaName is null) throw new ArgumentNullException(nameof(schemaName));
        if (tableName is null) throw new ArgumentNullException(nameof(tableName));

        return Tables.FirstOrDefault(t =>
            string.Equals(t.SchemaName, schemaName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(t.TableName, tableName, StringComparison.OrdinalIgnoreCase));
    }
}

public sealed class TableSchema
{
    public TableSchema(string schemaName, string tableName, IReadOnlyList<ColumnSchema> columns, PrimaryKeySchema? primaryKey = null)
    {
        SchemaName = schemaName ?? throw new ArgumentNullException(nameof(schemaName));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        Columns = columns ?? throw new ArgumentNullException(nameof(columns));
        PrimaryKey = primaryKey;
    }

    public string SchemaName { get; }

    public string TableName { get; }

    public IReadOnlyList<ColumnSchema> Columns { get; }

    public PrimaryKeySchema? PrimaryKey { get; }

    public ColumnSchema? FindColumn(string columnName)
    {
        if (columnName is null) throw new ArgumentNullException(nameof(columnName));

        return Columns.FirstOrDefault(c =>
            string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
    }
}

public sealed class ColumnSchema
{
    public ColumnSchema(
        string name,
        CanonicalDataType type,
        string nativeType,
        bool isNullable,
        int? length = null,
        int? precision = null,
        int? scale = null)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Type = type;
        NativeType = nativeType ?? throw new ArgumentNullException(nameof(nativeType));
        IsNullable = isNullable;
        Length = length;
        Precision = precision;
        Scale = scale;
    }

    public string Name { get; }

    public CanonicalDataType Type { get; }

    public string NativeType { get; }

    public bool IsNullable { get; }

    public int? Length { get; }

    public int? Precision { get; }

    public int? Scale { get; }
}

public sealed class PrimaryKeySchema
{
    public PrimaryKeySchema(IReadOnlyList<string> columns)
    {
        if (columns is null) throw new ArgumentNullException(nameof(columns));
        if (columns.Count == 0) throw new ArgumentException("Primary key must contain at least one column.", nameof(columns));

        Columns = columns;
    }

    public IReadOnlyList<string> Columns { get; }
}

