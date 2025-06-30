using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// ORDER BY句内容構築ビルダー
/// 設計理由：責務分離設計に準拠、キーワード除外で純粋なソート内容のみ生成
/// 出力例: "col1 ASC, col2 DESC" (ORDER BY除外)
/// </summary>
internal class OrderByClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.OrderBy;

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // 他Builderに依存しない
    }

    protected override string BuildInternal(Expression expression)
    {
        var visitor = new OrderByExpressionVisitor();
        visitor.Visit(expression);
        
        var result = visitor.GetResult();
        
        if (string.IsNullOrWhiteSpace(result))
        {
            throw new InvalidOperationException("Unable to extract ORDER BY columns from expression");
        }

        return result;
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // ORDER BY句特有のバリデーション
        ValidateOrderByLimitations(expression);
        ValidateOrderByColumns(expression);
    }

    /// <summary>
    /// ORDER BY制限チェック
    /// </summary>
    private static void ValidateOrderByLimitations(Expression expression)
    {
        // KSQLでのORDER BY制限
        Console.WriteLine("[KSQL-LINQ INFO] ORDER BY in KSQL is limited to Pull Queries and specific scenarios. " +
                         "Push Queries (streaming) do not guarantee order due to distributed processing.");

        // 複雑なソート式の制限
        var visitor = new OrderByComplexityVisitor();
        visitor.Visit(expression);
        
        if (visitor.HasComplexExpressions)
        {
            throw new InvalidOperationException(
                "ORDER BY in KSQL should use simple column references. " +
                "Complex expressions in ORDER BY may not be supported.");
        }
    }

    /// <summary>
    /// ORDER BYカラム数制限チェック
    /// </summary>
    private static void ValidateOrderByColumns(Expression expression)
    {
        var visitor = new OrderByColumnCountVisitor();
        visitor.Visit(expression);
        
        const int maxColumns = 5; // KSQL推奨制限
        if (visitor.ColumnCount > maxColumns)
        {
            throw new InvalidOperationException(
                $"ORDER BY supports maximum {maxColumns} columns for optimal performance. " +
                $"Found {visitor.ColumnCount} columns. Consider reducing sort columns.");
        }
    }
}

/// <summary>
/// ORDER BY句専用ExpressionVisitor
/// </summary>
internal class OrderByExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _orderClauses = new();

    public string GetResult()
    {
        return _orderClauses.Count > 0 ? string.Join(", ", _orderClauses) : string.Empty;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        switch (methodName)
        {
            case "OrderBy":
                ProcessOrderByCall(node, "ASC");
                break;

            case "OrderByDescending":
                ProcessOrderByCall(node, "DESC");
                break;

            case "ThenBy":
                ProcessThenByCall(node, "ASC");
                break;

            case "ThenByDescending":
                ProcessThenByCall(node, "DESC");
                break;

            default:
                // 他のメソッド呼び出しは再帰的に処理
                base.VisitMethodCall(node);
                break;
        }

        return node;
    }

    /// <summary>
    /// OrderBy/OrderByDescending 処理
    /// </summary>
    private void ProcessOrderByCall(MethodCallExpression node, string direction)
    {
        // 前のメソッドチェーンを先に処理
        if (node.Object != null)
        {
            Visit(node.Object);
        }

        // 現在のOrderBy処理
        if (node.Arguments.Count >= 2)
        {
            var keySelector = ExtractLambdaExpression(node.Arguments[1]);
            if (keySelector != null)
            {
                var columnName = ExtractColumnName(keySelector.Body);
                _orderClauses.Add($"{columnName} {direction}");
            }
        }
    }

    /// <summary>
    /// ThenBy/ThenByDescending 処理
    /// </summary>
    private void ProcessThenByCall(MethodCallExpression node, string direction)
    {
        // 前のメソッドチェーンを先に処理
        if (node.Object != null)
        {
            Visit(node.Object);
        }

        // 現在のThenBy処理
        if (node.Arguments.Count >= 2)
        {
            var keySelector = ExtractLambdaExpression(node.Arguments[1]);
            if (keySelector != null)
            {
                var columnName = ExtractColumnName(keySelector.Body);
                _orderClauses.Add($"{columnName} {direction}");
            }
        }
    }

    /// <summary>
    /// Lambda式抽出
    /// </summary>
    private static LambdaExpression? ExtractLambdaExpression(Expression expr)
    {
        return expr switch
        {
            UnaryExpression unary when unary.Operand is LambdaExpression lambda => lambda,
            LambdaExpression lambda => lambda,
            _ => null
        };
    }

    /// <summary>
    /// カラム名抽出
    /// </summary>
    private string ExtractColumnName(Expression expr)
    {
        return expr switch
        {
            MemberExpression member => GetMemberName(member),
            UnaryExpression unary when unary.NodeType == ExpressionType.Convert => ExtractColumnName(unary.Operand),
            MethodCallExpression method => ProcessOrderByFunction(method),
            _ => throw new InvalidOperationException($"Unsupported ORDER BY expression: {expr.GetType().Name}")
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
    /// ORDER BY関数処理
    /// </summary>
    private string ProcessOrderByFunction(MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;

        // ORDER BYで許可される関数は限定的
        return methodName switch
        {
            // ウィンドウ関数
            "WindowStart" => "WINDOWSTART",
            "WindowEnd" => "WINDOWEND",
            "RowTime" => "ROWTIME",
            
            // 文字列関数（部分的）
            "ToUpper" => ProcessSimpleFunction("UPPER", methodCall),
            "ToLower" => ProcessSimpleFunction("LOWER", methodCall),
            
            // 数値関数（部分的）
            "Abs" => ProcessSimpleFunction("ABS", methodCall),
            
            // 日付関数（部分的）
            "Year" => ProcessSimpleFunction("YEAR", methodCall),
            "Month" => ProcessSimpleFunction("MONTH", methodCall),
            "Day" => ProcessSimpleFunction("DAY", methodCall),
            
            _ => throw new InvalidOperationException($"Function '{methodName}' is not supported in ORDER BY clause")
        };
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
}

/// <summary>
/// ORDER BY複雑度チェックVisitor
/// </summary>
internal class OrderByComplexityVisitor : ExpressionVisitor
{
    public bool HasComplexExpressions { get; private set; }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        // 二項演算は複雑な式とみなす
        HasComplexExpressions = true;
        return base.VisitBinary(node);
    }

    protected override Expression VisitConditional(ConditionalExpression node)
    {
        // 条件式は複雑な式とみなす
        HasComplexExpressions = true;
        return base.VisitConditional(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;
        
        // 許可された関数以外は複雑とみなす
        var allowedMethods = new[]
        {
            "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending",
            "WindowStart", "WindowEnd", "RowTime",
            "ToUpper", "ToLower", "Abs", "Year", "Month", "Day"
        };

        if (!allowedMethods.Contains(methodName))
        {
            HasComplexExpressions = true;
        }

        return base.VisitMethodCall(node);
    }
}

/// <summary>
/// ORDER BYカラム数カウントVisitor
/// </summary>
internal class OrderByColumnCountVisitor : ExpressionVisitor
{
    public int ColumnCount { get; private set; }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        // ORDER BY系メソッドの場合はカウント増加
        if (methodName is "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending")
        {
            ColumnCount++;
        }

        return base.VisitMethodCall(node);
    }
}
