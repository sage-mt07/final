using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;


namespace Kafka.Ksql.Linq.Query.Pipeline;

internal static class WindowDDLExtensions
{
    internal static string GenerateCreateWindowTable(
        this DDLQueryGenerator generator,
        string windowTableName,
        string sourceTableName,
        int windowMinutes,
        Expression? groupByExpression = null,
        Expression? aggregationExpression = null,
        TimeSpan? gracePeriod = null)
    {
        var gracePeriodSeconds = gracePeriod?.TotalSeconds ?? 3;

        var selectClause = GenerateWindowSelectClause(aggregationExpression);
        var groupByClause = GenerateWindowGroupByClause(groupByExpression);
        var windowClause = $"WINDOW TUMBLING (SIZE {windowMinutes} MINUTES, GRACE PERIOD {gracePeriodSeconds} SECONDS)";

        var query = new StringBuilder();
        query.AppendLine($"CREATE TABLE {windowTableName} AS");
        query.AppendLine($"SELECT {selectClause}");
        query.AppendLine($"FROM {sourceTableName}");
        query.AppendLine(windowClause);

        if (!string.IsNullOrEmpty(groupByClause))
        {
            query.AppendLine(groupByClause);
        }

        query.Append("EMIT CHANGES");

        return query.ToString();
    }

    private static string GenerateWindowSelectClause(Expression? aggregationExpression)
    {
        if (aggregationExpression == null)
        {
            return "*";
        }

        var visitor = new WindowSelectExpressionVisitor();
        visitor.Visit(aggregationExpression);

        var selectColumns = visitor.GetSelectColumns();

        if (selectColumns.Count == 0)
        {
            return "*";
        }

        return string.Join(", ", selectColumns);
    }

    private static string GenerateWindowGroupByClause(Expression? groupByExpression)
    {
        if (groupByExpression == null)
        {
            return "";
        }

        var visitor = new WindowGroupByExpressionVisitor();
        visitor.Visit(groupByExpression);

        var groupByColumns = visitor.GetGroupByColumns();

        if (groupByColumns.Count == 0)
        {
            return "";
        }

        return $"GROUP BY {string.Join(", ", groupByColumns)}";
    }

    internal static string GenerateWindowAvroSchema(
        this DDLQueryGenerator generator,
        EntityModel entityModel,
        string windowTableName)
    {
        var timestampProperty = entityModel.GetTimestampProperty();
        if (timestampProperty == null)
        {
            throw new InvalidOperationException(
                $"Entity {entityModel.EntityType.Name} must have [AvroTimestamp] property for window operations");
        }

        var schema = new StringBuilder();
        schema.AppendLine("{");
        schema.AppendLine("  \"type\": \"record\",");
        schema.AppendLine($"  \"name\": \"{windowTableName}\",");
        schema.AppendLine("  \"fields\": [");

        var fields = entityModel.AllProperties
            .Where(p => p.GetCustomAttribute<KafkaIgnoreAttribute>() == null)
            .Select(p => GenerateAvroField(p))
            .ToList();

        fields.Add("    {\"name\": \"WINDOWSTART\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}");
        fields.Add("    {\"name\": \"WINDOWEND\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}");

        schema.AppendLine(string.Join(",\n", fields));
        schema.AppendLine("  ]");
        schema.AppendLine("}");

        return schema.ToString();
    }

    private static string GenerateAvroField(PropertyInfo property)
    {
        var fieldName = property.Name;
        var avroType = MapPropertyTypeToAvroType(property);

        if (property.GetCustomAttribute<AvroTimestampAttribute>() != null)
        {
            return $"    {{\"name\": \"{fieldName}\", \"type\": {{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}}}";
        }

        return $"    {{\"name\": \"{fieldName}\", \"type\": \"{avroType}\"}}";
    }

    private static string MapPropertyTypeToAvroType(PropertyInfo property)
    {
        var type = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

        return type switch
        {
            Type t when t == typeof(int) => "int",
            Type t when t == typeof(short) => "int",
            Type t when t == typeof(long) => "long",
            Type t when t == typeof(double) => "double",
            Type t when t == typeof(float) => "float",
            Type t when t == typeof(bool) => "boolean",
            Type t when t == typeof(string) => "string",
            Type t when t == typeof(char) => "string",
            Type t when t == typeof(DateTime) => "long",
            Type t when t == typeof(DateTimeOffset) => "long",
            Type t when t == typeof(decimal) => "bytes",
            Type t when t == typeof(Guid) => "string",
            Type t when t == typeof(byte[]) => "bytes",
            _ => "string"
        };
    }
}

internal class WindowSelectExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _selectColumns = new();

    internal List<string> GetSelectColumns() => _selectColumns;

    protected override Expression VisitNew(NewExpression node)
    {
        for (int i = 0; i < node.Arguments.Count; i++)
        {
            var arg = node.Arguments[i];
            var alias = node.Members?[i]?.Name ?? $"col{i}";

            var columnExpression = GenerateColumnExpression(arg);
            _selectColumns.Add($"{columnExpression} AS {alias}");
        }

        return base.VisitNew(node);
    }

    private string GenerateColumnExpression(Expression expression)
    {
        return expression switch
        {
            MethodCallExpression methodCall => GenerateAggregationFunction(methodCall),
            MemberExpression member => member.Member.Name.ToUpper(),
            ConstantExpression constant => $"'{constant.Value}'",
            _ => "UNKNOWN"
        };
    }

    private string GenerateAggregationFunction(MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;

        return methodName switch
        {
            "WindowStart" => "WINDOWSTART",
            "WindowEnd" => "WINDOWEND",
            "Sum" => ExtractAggregationColumn("SUM", methodCall),
            "Count" => "COUNT(*)",
            "Max" => ExtractAggregationColumn("MAX", methodCall),
            "Min" => ExtractAggregationColumn("MIN", methodCall),
            "Average" => ExtractAggregationColumn("AVG", methodCall),
            "Key" => ExtractKeyExpression(methodCall),
            _ => $"{methodName}(UNKNOWN)"
        };
    }

    private string ExtractAggregationColumn(string function, MethodCallExpression methodCall)
    {
        LambdaExpression? lambda = null;

        if (methodCall.Arguments.Count > 0)
        {
            lambda = ExtractLambda(methodCall.Arguments[0]);
        }

        if (lambda == null && methodCall.Arguments.Count > 1)
        {
            lambda = ExtractLambda(methodCall.Arguments[1]);
        }

        if (lambda?.Body is MemberExpression member)
        {
            return $"{function}({member.Member.Name.ToUpper()})";
        }

        return $"{function}(*)";
    }

    private static LambdaExpression? ExtractLambda(Expression expr)
    {
        return expr switch
        {
            LambdaExpression l => l,
            UnaryExpression u when u.Operand is LambdaExpression l => l,
            _ => null
        };
    }

    private string ExtractKeyExpression(MethodCallExpression methodCall)
    {
        if (methodCall.Object != null)
        {
            return "GROUPKEY";
        }

        return "KEY";
    }
}

internal class WindowGroupByExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _groupByColumns = new();

    internal List<string> GetGroupByColumns() => _groupByColumns;

    protected override Expression VisitMember(MemberExpression node)
    {
        _groupByColumns.Add(node.Member.Name.ToUpper());
        return base.VisitMember(node);
    }

    protected override Expression VisitNew(NewExpression node)
    {
        foreach (var arg in node.Arguments)
        {
            if (arg is MemberExpression member)
            {
                _groupByColumns.Add(member.Member.Name.ToUpper());
            }
        }
        return base.VisitNew(node);
    }
}
