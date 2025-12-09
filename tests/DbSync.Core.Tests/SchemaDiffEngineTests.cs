using System.Collections.Generic;
using DbSync.Core.Diff;
using DbSync.Core.Providers;
using DbSync.Core.Schema;

namespace DbSync.Core.Tests;

public class SchemaDiffEngineTests
{
    [Fact]
    public void DetectsMissingTableAndColumn()
    {
        var sourceTable = new TableSchema(
            "dbo",
            "Users",
            new List<ColumnSchema>
            {
                new("Id", CanonicalDataType.Int32, "int", isNullable: false),
                new("Name", CanonicalDataType.String, "nvarchar", isNullable: false, length: 200)
            },
            new PrimaryKeySchema(new[] { "Id" }));

        var targetTable = new TableSchema(
            "dbo",
            "Users",
            new List<ColumnSchema>
            {
                new("Id", CanonicalDataType.Int32, "int", isNullable: false)
            },
            new PrimaryKeySchema(new[] { "Id" }));

        var sourceSchema = new DatabaseSchema("SourceDb", new List<TableSchema> { sourceTable });
        var targetSchema = new DatabaseSchema("TargetDb", new List<TableSchema> { targetTable });

        var ddlGenerator = new TestDdlGenerator();

        var result = SchemaDiffEngine.Diff(sourceSchema, targetSchema, ddlGenerator);

        Assert.True(result.HasDifferences);
        Assert.Single(result.TableDifferences);

        var tableDiff = result.TableDifferences[0];
        Assert.Single(tableDiff.MissingColumns);
        Assert.Equal("Name", tableDiff.MissingColumns[0].SourceColumn?.Name);

        Assert.True(result.MigrationPlan.HasSteps);
        Assert.Single(result.MigrationPlan.Steps);
        Assert.Equal(MigrationStepKind.AddColumn, result.MigrationPlan.Steps[0].Kind);
    }

    private sealed class TestDdlGenerator : IDdlGenerator
    {
        public string GenerateCreateTable(TableSchema table) => $"CREATE TABLE {table.SchemaName}.{table.TableName}";

        public string GenerateAddColumn(TableSchema table, ColumnSchema column) =>
            $"ALTER TABLE {table.SchemaName}.{table.TableName} ADD {column.Name}";
    }
}

