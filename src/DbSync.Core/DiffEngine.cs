using System;
using System.Collections.Generic;
using System.Linq;
using DbSync.Core.Providers;
using DbSync.Core.Schema;

namespace DbSync.Core.Diff;

public enum ColumnDifferenceKind
{
    MissingInTarget,
    MissingInSource,
    TypeMismatch,
    NullabilityMismatch
}

public sealed class ColumnDifference
{
    public ColumnDifference(ColumnDifferenceKind kind, ColumnSchema? sourceColumn, ColumnSchema? targetColumn)
    {
        Kind = kind;
        SourceColumn = sourceColumn;
        TargetColumn = targetColumn;
    }

    public ColumnDifferenceKind Kind { get; }

    public ColumnSchema? SourceColumn { get; }

    public ColumnSchema? TargetColumn { get; }
}

public sealed class TableDifference
{
    public TableDifference(string schemaName, string tableName)
    {
        SchemaName = schemaName ?? throw new ArgumentNullException(nameof(schemaName));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        MissingColumns = new List<ColumnDifference>();
        ExtraColumns = new List<ColumnDifference>();
        MismatchedColumns = new List<ColumnDifference>();
    }

    public string SchemaName { get; }

    public string TableName { get; }

    public bool IsMissingInTarget { get; init; }

    public bool IsExtraInTarget { get; init; }

    public List<ColumnDifference> MissingColumns { get; }

    public List<ColumnDifference> ExtraColumns { get; }

    public List<ColumnDifference> MismatchedColumns { get; }
}

public enum MigrationStepKind
{
    CreateTable,
    AddColumn
}

public enum RiskLevel
{
    Low,
    Medium,
    High
}

public sealed class MigrationStep
{
    public MigrationStep(
        MigrationStepKind kind,
        string schemaName,
        string tableName,
        string sql,
        ColumnSchema? column,
        RiskLevel risk)
    {
        Kind = kind;
        SchemaName = schemaName ?? throw new ArgumentNullException(nameof(schemaName));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
        Column = column;
        Risk = risk;
    }

    public MigrationStepKind Kind { get; }

    public string SchemaName { get; }

    public string TableName { get; }

    public ColumnSchema? Column { get; }

    public string Sql { get; }

    public RiskLevel Risk { get; }

    public string? AiSql { get; set; }

    public string? AiReasoning { get; set; }
}

public sealed class MigrationPlan
{
    private readonly List<MigrationStep> _steps = new();

    public IReadOnlyList<MigrationStep> Steps => _steps;

    public bool HasSteps => _steps.Count > 0;

    public void AddStep(MigrationStep step)
    {
        if (step is null) throw new ArgumentNullException(nameof(step));
        _steps.Add(step);
    }
}

public sealed class SchemaDiffResult
{
    public List<TableDifference> TableDifferences { get; } = new();

    public MigrationPlan MigrationPlan { get; } = new();

    public bool HasDifferences =>
        TableDifferences.Any(t =>
            t.IsMissingInTarget ||
            t.IsExtraInTarget ||
            t.MissingColumns.Count > 0 ||
            t.ExtraColumns.Count > 0 ||
            t.MismatchedColumns.Count > 0);
}

public static class SchemaDiffEngine
{
    public static SchemaDiffResult Diff(DatabaseSchema source, DatabaseSchema target, IDdlGenerator targetDdlGenerator)
    {
        if (source is null) throw new ArgumentNullException(nameof(source));
        if (target is null) throw new ArgumentNullException(nameof(target));
        if (targetDdlGenerator is null) throw new ArgumentNullException(nameof(targetDdlGenerator));

        var result = new SchemaDiffResult();

        // Compare tables present in the source.
        foreach (var sourceTable in source.Tables)
        {
            var targetTable = target.FindTable(sourceTable.SchemaName, sourceTable.TableName);
            if (targetTable is null)
            {
                var createSql = targetDdlGenerator.GenerateCreateTable(sourceTable);
                var createStep = new MigrationStep(
                    MigrationStepKind.CreateTable,
                    sourceTable.SchemaName,
                    sourceTable.TableName,
                    createSql,
                    null,
                    RiskLevel.Low);

                result.MigrationPlan.AddStep(createStep);

                var tableDiff = new TableDifference(sourceTable.SchemaName, sourceTable.TableName)
                {
                    IsMissingInTarget = true
                };

                result.TableDifferences.Add(tableDiff);
                continue;
            }

            var diff = new TableDifference(sourceTable.SchemaName, sourceTable.TableName);

            // Missing columns in target.
            foreach (var sourceColumn in sourceTable.Columns)
            {
                var targetColumn = targetTable.FindColumn(sourceColumn.Name);
                if (targetColumn is null)
                {
                    var addSql = targetDdlGenerator.GenerateAddColumn(sourceTable, sourceColumn);
                    var addStep = new MigrationStep(
                        MigrationStepKind.AddColumn,
                        sourceTable.SchemaName,
                        sourceTable.TableName,
                        addSql,
                        sourceColumn,
                        RiskLevel.Low);

                    result.MigrationPlan.AddStep(addStep);

                    diff.MissingColumns.Add(new ColumnDifference(ColumnDifferenceKind.MissingInTarget, sourceColumn, null));
                }
                else
                {
                    if (sourceColumn.Type != targetColumn.Type)
                    {
                        diff.MismatchedColumns.Add(
                            new ColumnDifference(ColumnDifferenceKind.TypeMismatch, sourceColumn, targetColumn));
                    }

                    if (sourceColumn.IsNullable != targetColumn.IsNullable)
                    {
                        diff.MismatchedColumns.Add(
                            new ColumnDifference(ColumnDifferenceKind.NullabilityMismatch, sourceColumn, targetColumn));
                    }
                }
            }

            // Extra columns in target.
            foreach (var targetColumn in targetTable.Columns)
            {
                var sourceColumn = sourceTable.FindColumn(targetColumn.Name);
                if (sourceColumn is null)
                {
                    diff.ExtraColumns.Add(new ColumnDifference(ColumnDifferenceKind.MissingInSource, null, targetColumn));
                }
            }

            if (diff.IsMissingInTarget || diff.IsExtraInTarget ||
                diff.MissingColumns.Count > 0 ||
                diff.ExtraColumns.Count > 0 ||
                diff.MismatchedColumns.Count > 0)
            {
                result.TableDifferences.Add(diff);
            }
        }

        // Tables present only in the target.
        foreach (var targetTable in target.Tables)
        {
            var sourceTable = source.FindTable(targetTable.SchemaName, targetTable.TableName);
            if (sourceTable is null)
            {
                var diff = new TableDifference(targetTable.SchemaName, targetTable.TableName)
                {
                    IsExtraInTarget = true
                };

                result.TableDifferences.Add(diff);
            }
        }

        return result;
    }
}

