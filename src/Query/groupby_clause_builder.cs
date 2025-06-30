using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// GROUP BY句内容構築ビルダー
/// 設計理由：責務分離設計に準拠、キーワード除外で純粋なグループ化キー内容のみ生成
/// 出力例: "col1, col2" (GROUP BY除外)
/// </summary>
internal class GroupByClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.GroupBy;

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // 他Builderに依存しない
    }

    protected override string BuildInternal(Expression expression)
    {
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expression);
        
        var result = visitor.GetResult();
        
        if (string.IsNullOrWhiteSpace(result))
        {
            throw new InvalidOperationException("Unable to extract GROUP BY keys from expression");
        }

        return result;
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // GROUP BY句特有のバリデーション
        ValidateNoAggregateInGroupBy(expression);
        ValidateGroupByKeyCount(expression);
    }

    /// <summary>
    /// GROUP BY句での集約関数使用禁止チェック
    /// </summary>
    private static void ValidateNoAggregateInGroupBy(Expression expression)
    {
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(expression);
        
        if (visitor.HasAggregates)
        {
            throw new InvalidOperationException(
                "Aggregate functions are not allowed in GROUP BY clause");
        }
    }

    /// <summary>
    /// GROUP BYキー数制限チェック
    /// </summary>
    private static void ValidateGroupByKeyCount(Expression expression)
    {
        var visitor = new GroupByKeyCountVisitor();
        visitor.Visit(expression);
        
        const int maxKeys = 10; // KSQL推奨制限
        if (visitor.KeyCount > maxKeys)
        {
            throw new InvalidOperationException(
                $"GROUP BY supports maximum {maxKeys} keys for optimal performance. " +
                $"Found {visitor.KeyCount} keys. Consider using composite keys or data denormalization.");
        }
    }
}

/// <summary>
/// GROUP BY句専用ExpressionVisitor
/// </summary>
internal class GroupByExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _keys = new();

    public string GetResult()
    {
        return _keys.Count > 0 ? string.Join(", ", _keys) : string.Empty;
    }

    protected override Expression VisitNew(NewExpression node)
    {
        // 複合キーの処理（匿名型）
        foreach (var arg in node.Arguments)
        {
            var key = ExtractGroupByKey(arg);
            if (!string.IsNullOrEmpty(key))
            {
                _keys.Add(key);
            }
        }
        
        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // 単一キーの処理
        var key = ExtractGroupByKey(node);
        if (!string.IsNullOrEmpty(key))
        {
            _keys.Add(key);
        }
        
        return node;
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        // 型変換の処理（Convert等）
        if (node.NodeType == ExpressionType.Convert || node.NodeType == ExpressionType.ConvertChecked)
        {
            return Visit(node.Operand);
        }

        return base.VisitUnary(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // GROUP BYで使用可能な関数（日付部分抽出等）
        var methodName = node.Method.Name;
        
        if (IsAllowedGroupByFunction(methodName))
        {
            var functionCall = ProcessGroupByFunction(node);
            _keys.Add(functionCall);
            return node;
        }

        // 許可されていない関数
        throw new InvalidOperationException(
            $"Function '{methodName}' is not allowed in GROUP BY clause");
    }

    /// <summary>
    /// GROUP BYキー抽出
    /// </summary>
    private string ExtractGroupByKey(Expression expr)
    {
        return expr switch
        {
            MemberExpression member => GetMemberName(member),
            UnaryExpression unary when unary.NodeType == ExpressionType.Convert => ExtractGroupByKey(unary.Operand),
            MethodCallExpression method when IsAllowedGroupByFunction(method.Method.Name) => ProcessGroupByFunction(method),
            _ => throw new InvalidOperationException($"Expression type '{expr.GetType().Name}' is not supported in GROUP BY")
        };
    }

    /// <summary>
    /// メンバー名取得
    /// </summary>
    private static string GetMemberName(MemberExpression member)
    {
        // ネストしたプロパティの場合は最下位のプロパティ名を使用
        return member.Member.Name;
    }

    /// <summary>
    /// GROUP BYで許可された関数判定
    /// </summary>
    private static bool IsAllowedGroupByFunction(string methodName)
    {
        var allowedFunctions = new[]
        {
            // 日付関数
            "Year", "Month", "Day", "Hour", "Minute", "Second",
            "DayOfWeek", "DayOfYear", "WeekOfYear",
            
            // 文字列関数（部分）
            "Substring", "Left", "Right", "ToUpper", "ToLower",
            
            // 数値関数（部分）
            "Floor", "Ceiling", "Round",
            
            // 型変換
            "ToString", "Cast"
        };

        return allowedFunctions.Contains(methodName);
    }

    /// <summary>
    /// GROUP BY関数処理
    /// </summary>
    private string ProcessGroupByFunction(MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;

        return methodName switch
        {
            // 日付関数
            "Year" => ProcessDateFunction("YEAR", methodCall),
            "Month" => ProcessDateFunction("MONTH", methodCall),
            "Day" => ProcessDateFunction("DAY", methodCall),
            "Hour" => ProcessDateFunction("HOUR", methodCall),
            "Minute" => ProcessDateFunction("MINUTE", methodCall),
            "Second" => ProcessDateFunction("SECOND", methodCall),
            "DayOfWeek" => ProcessDateFunction("DAY_OF_WEEK", methodCall),
            "DayOfYear" => ProcessDateFunction("DAY_OF_YEAR", methodCall),
            "WeekOfYear" => ProcessDateFunction("WEEK_OF_YEAR", methodCall),

            // 文字列関数
            "Substring" => ProcessSubstringFunction(methodCall),
            "Left" => ProcessLeftFunction(methodCall),
            "Right" => ProcessRightFunction(methodCall),
            "ToUpper" => ProcessSimpleFunction("UPPER", methodCall),
            "ToLower" => ProcessSimpleFunction("LOWER", methodCall),

            // 数値関数
            "Floor" => ProcessSimpleFunction("FLOOR", methodCall),
            "Ceiling" => ProcessSimpleFunction("CEIL", methodCall),
            "Round" => ProcessRoundFunction(methodCall),

            // 型変換
            "ToString" => ProcessToStringFunction(methodCall),

            _ => throw new InvalidOperationException($"Unsupported GROUP BY function: {methodName}")
        };
    }

    /// <summary>
    /// 日付関数処理
    /// </summary>
    private string ProcessDateFunction(string ksqlFunction, MethodCallExpression methodCall)
    {
        var target = methodCall.Object ?? methodCall.Arguments[0];
        var columnName = ExtractColumnName(target);
        return $"{ksqlFunction}({columnName})";
    }

    /// <summary>
    /// 単純関数処理
    /// </summary>
    private string ProcessSimpleFunction(string ksqlFunction, MethodCallExpression methodCall)
    {
        var target = methodCall.Object ?? methodCall.Arguments[0];
        var columnName = ExtractColumnName(target);
        return $"{ksqlFunction}({columnName})";
    }

    /// <summary>
    /// SUBSTRING関数処理
    /// </summary>
    private string ProcessSubstringFunction(MethodCallExpression methodCall)
    {
        var target = methodCall.Object ?? methodCall.Arguments[0];
        var columnName = ExtractColumnName(target);
        
        if (methodCall.Arguments.Count >= 2)
        {
            var startIndex = ExtractConstantValue(methodCall.Arguments[methodCall.Object != null ? 0 : 1]);
            
            if (methodCall.Arguments.Count >= 3)
            {
                var length = ExtractConstantValue(methodCall.Arguments[methodCall.Object != null ? 1 : 2]);
                return $"SUBSTRING({columnName}, {startIndex}, {length})";
            }
            
            return $"SUBSTRING({columnName}, {startIndex})";
        }
        
        throw new InvalidOperationException("SUBSTRING requires at least start index parameter");
    }

    /// <summary>
    /// LEFT関数処理
    /// </summary>
    private string ProcessLeftFunction(MethodCallExpression methodCall)
    {
        var target = methodCall.Object ?? methodCall.Arguments[0];
        var columnName = ExtractColumnName(target);
        var length = ExtractConstantValue(methodCall.Arguments[methodCall.Object != null ? 0 : 1]);
        return $"LEFT({columnName}, {length})";
    }

    /// <summary>
    /// RIGHT関数処理
    /// </summary>
    private string ProcessRightFunction(MethodCallExpression methodCall)
    {
        var target = methodCall.Object ?? methodCall.Arguments[0];
        var columnName = ExtractColumnName(target);
        var length = ExtractConstantValue(methodCall.Arguments[methodCall.Object != null ? 0 : 1]);
        return $"RIGHT({columnName}, {length})";
    }

    /// <summary>
    /// ROUND関数処理
    /// </summary>
    private string ProcessRoundFunction(MethodCallExpression methodCall)
    {
        var target = methodCall.Object ?? methodCall.Arguments[0];
        var columnName = ExtractColumnName(target);
        
        if (methodCall.Arguments.Count >= 2)
        {
            var precision = ExtractConstantValue(methodCall.Arguments[methodCall.Object != null ? 0 : 1]);
            return $"ROUND({columnName}, {precision})";
        }
        
        return $"ROUND({columnName})";
    }

    /// <summary>
    /// ToString関数処理
    /// </summary>
    private string ProcessToStringFunction(MethodCallExpression methodCall)
    {
        var target = methodCall.Object ?? methodCall.Arguments[0];
        var columnName = ExtractColumnName(target);
        return $"CAST({columnName} AS VARCHAR)";
    }

    /// <summary>
    /// カラム名抽出
    /// </summary>
    private string ExtractColumnName(Expression expression)
    {
        return expression switch
        {
            MemberExpression member => member.Member.Name,
            UnaryExpression unary => ExtractColumnName(unary.Operand),
            _ => throw new InvalidOperationException($"Cannot extract column name from {expression.GetType().Name}")
        };
    }

    /// <summary>
    /// 定数値抽出
    /// </summary>
    private string ExtractConstantValue(Expression expression)
    {
        return expression switch
        {
            ConstantExpression constant => constant.Value?.ToString() ?? "NULL",
            UnaryExpression unary => ExtractConstantValue(unary.Operand),
            _ => throw new InvalidOperationException($"Expected constant value but got {expression.GetType().Name}")
        };
    }
}

/// <summary>
/// GROUP BYキー数カウントVisitor
/// </summary>
internal class GroupByKeyCountVisitor : ExpressionVisitor
{
    public int KeyCount { get; private set; }

    protected override Expression VisitNew(NewExpression node)
    {
        KeyCount += node.Arguments.Count;
        return base.VisitNew(node);
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // NewExpression内でない単独のMemberは1つのキー
        if (KeyCount == 0)
        {
            KeyCount = 1;
        }
        return base.VisitMember(node);
    }
}
