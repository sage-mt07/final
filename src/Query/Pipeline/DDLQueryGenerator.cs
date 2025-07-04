using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Builders.Common;
using Kafka.Ksql.Linq.Core.Modeling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// DDLクエリ生成器（新Builder使用版）
/// 設計理由：責務分離設計に準拠、Builder統合型でCREATE STREAM/TABLE文生成
/// </summary>
internal class DDLQueryGenerator : GeneratorBase, IDDLQueryGenerator
{
    /// <summary>
    /// コンストラクタ（Builder依存注入）
    /// </summary>
    public DDLQueryGenerator(IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> builders)
        : base(builders)
    {
    }

    /// <summary>
    /// 簡易コンストラクタ（標準Builder使用）
    /// </summary>
    public DDLQueryGenerator() : this(CreateStandardBuilders())
    {
    }

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return new[]
        {
            KsqlBuilderType.Select,
            KsqlBuilderType.Where,
            KsqlBuilderType.GroupBy,
            KsqlBuilderType.Window
        };
    }

    /// <summary>
    /// CREATE STREAM文生成
    /// </summary>
    public string GenerateCreateStream(string streamName, string topicName, EntityModel entityModel)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var columns = GenerateColumnDefinitions(entityModel);
            var query = $"CREATE STREAM {streamName} ({columns}) WITH (KAFKA_TOPIC='{topicName}', VALUE_FORMAT='AVRO')";

            return query;
        }
        catch (Exception ex)
        {
            return HandleGenerationError("CREATE STREAM generation", ex, $"Stream: {streamName}, Topic: {topicName}");
        }
    }

    /// <summary>
    /// CREATE TABLE文生成
    /// </summary>
    public string GenerateCreateTable(string tableName, string topicName, EntityModel entityModel)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var columns = GenerateColumnDefinitions(entityModel);
            var keyColumns = string.Join(", ", entityModel.KeyProperties.Select(p => p.Name.ToUpper()));

            var query = $"CREATE TABLE {tableName} ({columns}) WITH (KAFKA_TOPIC='{topicName}', VALUE_FORMAT='AVRO'";

            if (!string.IsNullOrEmpty(keyColumns))
            {
                query += $", KEY='{keyColumns}'";
            }

            query += ")";

            return query;
        }
        catch (Exception ex)
        {
            return HandleGenerationError("CREATE TABLE generation", ex, $"Table: {tableName}, Topic: {topicName}");
        }
    }

    /// <summary>
    /// CREATE STREAM AS文生成
    /// </summary>
    public string GenerateCreateStreamAs(string streamName, string baseObject, Expression linqExpression)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var context = new QueryAssemblyContext(baseObject, false); // Push Query
            var structure = CreateStreamAsStructure(streamName, baseObject);

            // LINQ式を解析してクエリ句を構築
            structure = ProcessLinqExpression(structure, linqExpression, context);

            var query = AssembleStructuredQuery(structure);
            return ApplyQueryPostProcessing(query, context);
        }
        catch (Exception ex)
        {
            return HandleGenerationError("CREATE STREAM AS generation", ex, $"Stream: {streamName}, Base: {baseObject}");
        }
    }

    /// <summary>
    /// CREATE TABLE AS文生成
    /// </summary>
    public string GenerateCreateTableAs(string tableName, string baseObject, Expression linqExpression)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var context = new QueryAssemblyContext(baseObject, false); // Push Query
            var structure = CreateTableAsStructure(tableName, baseObject);

            // LINQ式を解析してクエリ句を構築
            structure = ProcessLinqExpression(structure, linqExpression, context);

            var query = AssembleStructuredQuery(structure);
            return ApplyQueryPostProcessing(query, context);
        }
        catch (Exception ex)
        {
            return HandleGenerationError("CREATE TABLE AS generation", ex, $"Table: {tableName}, Base: {baseObject}");
        }
    }

    /// <summary>
    /// カラム定義生成
    /// </summary>
    private string GenerateColumnDefinitions(EntityModel entityModel)
    {
        var columns = new List<string>();

        foreach (var property in entityModel.EntityType.GetProperties())
        {
            if (property.GetCustomAttribute<KafkaIgnoreAttribute>() != null)
                continue;

            var columnName = property.Name.ToUpper();
            var ksqlType = MapToKsqlType(property.PropertyType);
            columns.Add($"{columnName} {ksqlType}");
        }

        return string.Join(", ", columns);
    }

    /// <summary>
    /// C#型からKSQL型マッピング
    /// </summary>
    private static string MapToKsqlType(Type propertyType)
    {
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        return underlyingType switch
        {
            Type t when t == typeof(int) => "INTEGER",
            Type t when t == typeof(short) => "INTEGER",
            Type t when t == typeof(long) => "BIGINT",
            Type t when t == typeof(double) => "DOUBLE",
            Type t when t == typeof(float) => "DOUBLE",
            Type t when t == typeof(decimal) => "DECIMAL(38, 9)",
            Type t when t == typeof(string) => "VARCHAR",
            Type t when t == typeof(char) => "VARCHAR",
            Type t when t == typeof(bool) => "BOOLEAN",
            Type t when t == typeof(DateTime) => "TIMESTAMP",
            Type t when t == typeof(DateTimeOffset) => "TIMESTAMP",
            Type t when t == typeof(Guid) => "VARCHAR",
            Type t when t == typeof(byte[]) => "BYTES",
            _ when underlyingType.IsEnum => throw new NotSupportedException($"Type '{underlyingType.Name}' is not supported."),
            _ when !underlyingType.IsPrimitive && underlyingType != typeof(string) && underlyingType != typeof(char) && underlyingType != typeof(Guid) && underlyingType != typeof(byte[]) => throw new NotSupportedException($"Type '{underlyingType.Name}' is not supported."),
            _ => throw new NotSupportedException($"Type '{underlyingType.Name}' is not supported.")
        };
    }

    /// <summary>
    /// CREATE STREAM AS構造作成
    /// </summary>
    private static QueryStructure CreateStreamAsStructure(string streamName, string baseObject)
    {
        var metadata = new QueryMetadata(DateTime.UtcNow, "DDL", baseObject);
        var structure = QueryStructure.CreateStreamAs(streamName, baseObject).WithMetadata(metadata);
        var fromClause = QueryClause.Required(QueryClauseType.From, $"FROM {baseObject}");
        return structure.AddClause(fromClause);
    }

    /// <summary>
    /// CREATE TABLE AS構造作成
    /// </summary>
    private static QueryStructure CreateTableAsStructure(string tableName, string baseObject)
    {
        var metadata = new QueryMetadata(DateTime.UtcNow, "DDL", baseObject);
        var structure = QueryStructure.CreateTableAs(tableName, baseObject).WithMetadata(metadata);
        var fromClause = QueryClause.Required(QueryClauseType.From, $"FROM {baseObject}");
        return structure.AddClause(fromClause);
    }

    /// <summary>
    /// LINQ式処理
    /// </summary>
    private QueryStructure ProcessLinqExpression(QueryStructure structure, Expression linqExpression, QueryAssemblyContext context)
    {
        var analysis = AnalyzeLinqExpression(linqExpression);

        foreach (var methodCall in analysis.MethodCalls.AsEnumerable().Reverse())
        {
            structure = ProcessMethodCall(structure, methodCall, context);
        }

        return structure;
    }

    /// <summary>
    /// メソッド呼び出し処理
    /// </summary>
    private QueryStructure ProcessMethodCall(QueryStructure structure, MethodCallExpression methodCall, QueryAssemblyContext context)
    {
        var methodName = methodCall.Method.Name;

        return methodName switch
        {
            "Select" => ProcessSelectMethod(structure, methodCall),
            "Where" => ProcessWhereMethod(structure, methodCall),
            "GroupBy" => ProcessGroupByMethod(structure, methodCall),
            "Window" => ProcessWindowMethod(structure, methodCall),
            "Having" => ProcessHavingMethod(structure, methodCall),
            "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" => ProcessOrderByMethod(structure, methodCall),
            _ => structure // 未対応メソッドは無視
        };
    }

    /// <summary>
    /// SELECT メソッド処理
    /// </summary>
    private QueryStructure ProcessSelectMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var selectContent = SafeCallBuilder(KsqlBuilderType.Select, lambdaBody, "SELECT processing");
                var clause = QueryClause.Required(QueryClauseType.Select, selectContent, lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// WHERE メソッド処理
    /// </summary>
    private QueryStructure ProcessWhereMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var whereContent = SafeCallBuilder(KsqlBuilderType.Where, lambdaBody, "WHERE processing");
                var clause = QueryClause.Required(QueryClauseType.Where, $"WHERE {whereContent}", lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// GROUP BY メソッド処理
    /// </summary>
    private QueryStructure ProcessGroupByMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var groupByContent = SafeCallBuilder(KsqlBuilderType.GroupBy, lambdaBody, "GROUP BY processing");
                var clause = QueryClause.Required(QueryClauseType.GroupBy, $"GROUP BY {groupByContent}", lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// WINDOW メソッド処理
    /// </summary>
    private QueryStructure ProcessWindowMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var windowExpression = methodCall.Arguments[1];
            var windowContent = SafeCallBuilder(KsqlBuilderType.Window, windowExpression, "WINDOW processing");
            var clause = QueryClause.Required(QueryClauseType.Window, $"WINDOW {windowContent}", windowExpression);
            structure = structure.AddClause(clause);
        }

        return structure;
    }

    /// <summary>
    /// HAVING メソッド処理
    /// </summary>
    private QueryStructure ProcessHavingMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (HasBuilder(KsqlBuilderType.Having) && methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var havingContent = SafeCallBuilder(KsqlBuilderType.Having, lambdaBody, "HAVING processing");
                var clause = QueryClause.Required(QueryClauseType.Having, $"HAVING {havingContent}", lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// ORDER BY メソッド処理
    /// </summary>
    private QueryStructure ProcessOrderByMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (HasBuilder(KsqlBuilderType.OrderBy))
        {
            var orderByContent = SafeCallBuilder(KsqlBuilderType.OrderBy, methodCall, "ORDER BY processing");
            var clause = QueryClause.Optional(QueryClauseType.OrderBy, $"ORDER BY {orderByContent}", methodCall);
            structure = structure.AddClause(clause);
        }

        return structure;
    }

    /// <summary>
    /// LINQ式解析
    /// </summary>
    private ExpressionAnalysisResult AnalyzeLinqExpression(Expression expression)
    {
        var result = new ExpressionAnalysisResult();
        var visitor = new MethodCallCollectorVisitor();
        visitor.Visit(expression);
        result.MethodCalls = visitor.MethodCalls;
        return result;
    }

    /// <summary>
    /// Lambda Body抽出
    /// </summary>
    private static Expression? ExtractLambdaBody(Expression expression)
    {
        return BuilderValidation.ExtractLambdaBody(expression);
    }

    /// <summary>
    /// 標準Builder作成
    /// </summary>
    private static IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> CreateStandardBuilders()
    {
        return new Dictionary<KsqlBuilderType, IKsqlBuilder>
        {
            [KsqlBuilderType.Select] = new SelectClauseBuilder(),
            [KsqlBuilderType.Where] = new WhereClauseBuilder(),
            [KsqlBuilderType.GroupBy] = new GroupByClauseBuilder(),
            [KsqlBuilderType.Having] = new HavingClauseBuilder(),
            [KsqlBuilderType.Join] = new JoinClauseBuilder(),
            [KsqlBuilderType.Window] = new WindowClauseBuilder()
        };
    }
}
