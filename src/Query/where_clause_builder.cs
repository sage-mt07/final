using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using Kafka.Ksql.Linq.Query.Builders.Functions;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// WHERE句内容構築ビルダー
/// 設計理由：責務分離設計に準拠、キーワード除外で純粋な条件内容のみ生成
/// 出力例: "condition1 AND condition2" (WHERE除外)
/// </summary>
internal class WhereClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.Where;

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // 他Builderに依存しない
    }

    protected override string BuildInternal(Expression expression)
    {
        var visitor = new WhereExpressionVisitor();
        visitor.Visit(expression);
        return visitor.GetResult();
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // WHERE句特有のバリデーション
        ValidateNoAggregateInWhere(expression);
        ValidateNoSelectStatements(expression);
    }

    /// <summary>
    /// WHERE句での集約関数使用禁止チェック
    /// </summary>
    private static void ValidateNoAggregateInWhere(Expression expression)
    {
        var visitor = new AggregateDetectionVisitor();
        visitor.Visit(expression);
        
        if (visitor.HasAggregates)
        {
            throw new InvalidOperationException(
                "Aggregate functions are not allowed in WHERE clause. Use HAVING clause instead.");
        }
    }

    /// <summary>
    /// WHERE句でのSELECT文混入禁止チェック
    /// </summary>
    private static void ValidateNoSelectStatements(Expression expression)
    {
        // 基本的なサブクエリパターンの検出
        var expressionString = expression.ToString().ToUpper();
        if (expressionString.Contains("SELECT"))
        {
            throw new InvalidOperationException(
                "Subqueries are not supported in WHERE clause in KSQL");
        }
    }

    /// <summary>
    /// 条件のみ構築（WHERE プレフィックスなし）
    /// </summary>
    public string BuildCondition(Expression expression)
    {
        return BuildInternal(expression);
    }
}

/// <summary>
/// WHERE句専用ExpressionVisitor
/// </summary>
internal class WhereExpressionVisitor : ExpressionVisitor
{
    private readonly Stack<string> _conditionStack = new();
    private string _result = string.Empty;

    public string GetResult()
    {
        return _result;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        // NULL比較の特別処理
        if (IsNullComparison(node))
        {
            _result = HandleNullComparison(node);
            return node;
        }

        // 複合キー比較の処理
        if (IsCompositeKeyComparison(node))
        {
            _result = HandleCompositeKeyComparison(node);
            return node;
        }

        // 通常の二項演算処理
        var left = ProcessExpression(node.Left);
        var right = ProcessExpression(node.Right);
        var varoperator = GetSqlOperator(node.NodeType);

        _result = $"({left} {varoperator} {right})";
        return node;
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        switch (node.NodeType)
        {
            case ExpressionType.Not:
                _result = HandleNotExpression(node);
                break;

            case ExpressionType.Convert:
            case ExpressionType.ConvertChecked:
                // 型変換は内側の式を処理
                Visit(node.Operand);
                break;

            default:
                Visit(node.Operand);
                break;
        }

        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // プロパティアクセスの処理
        _result = HandleMemberAccess(node);
        return node;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        _result = SafeToString(node.Value);
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // メソッド呼び出しの処理
        _result = HandleMethodCall(node);
        return node;
    }

    /// <summary>
    /// NULL比較判定
    /// </summary>
    private static bool IsNullComparison(BinaryExpression node)
    {
        return (node.NodeType == ExpressionType.Equal || node.NodeType == ExpressionType.NotEqual) &&
               (IsNullConstant(node.Left) || IsNullConstant(node.Right));
    }

    /// <summary>
    /// NULL定数判定
    /// </summary>
    private static bool IsNullConstant(Expression expr)
    {
        return expr is ConstantExpression constant && constant.Value == null;
    }

    /// <summary>
    /// 複合キー比較判定
    /// </summary>
    private static bool IsCompositeKeyComparison(BinaryExpression node)
    {
        return node.NodeType == ExpressionType.Equal &&
               node.Left is NewExpression &&
               node.Right is NewExpression;
    }

    /// <summary>
    /// NULL比較処理
    /// </summary>
    private string HandleNullComparison(BinaryExpression node)
    {
        var memberExpr = IsNullConstant(node.Left) ? node.Right : node.Left;
        var memberName = ProcessExpression(memberExpr);
        var isNotEqual = node.NodeType == ExpressionType.NotEqual;

        return $"{memberName} IS {(isNotEqual ? "NOT " : "")}NULL";
    }

    /// <summary>
    /// 複合キー比較処理
    /// </summary>
    private string HandleCompositeKeyComparison(BinaryExpression node)
    {
        var leftNew = (NewExpression)node.Left;
        var rightNew = (NewExpression)node.Right;

        if (leftNew.Arguments.Count != rightNew.Arguments.Count)
        {
            throw new InvalidOperationException("Composite key expressions must have the same number of properties");
        }

        var conditions = new List<string>();

        for (int i = 0; i < leftNew.Arguments.Count; i++)
        {
            var leftMember = ProcessExpression(leftNew.Arguments[i]);
            var rightMember = ProcessExpression(rightNew.Arguments[i]);
            conditions.Add($"{leftMember} = {rightMember}");
        }

        return conditions.Count == 1 ? conditions[0] : $"({string.Join(" AND ", conditions)})";
    }

    /// <summary>
    /// NOT式処理
    /// </summary>
    private string HandleNotExpression(UnaryExpression node)
    {
        // Nullable<bool>の.Value アクセス処理
        if (node.Operand is MemberExpression member &&
            member.Member.Name == "Value" &&
            member.Expression is MemberExpression innerMember &&
            innerMember.Type == typeof(bool?))
        {
            var memberName = GetMemberName(innerMember);
            return $"({memberName} = false)";
        }

        // 通常のboolean否定
        if (node.Operand is MemberExpression regularMember &&
            (regularMember.Type == typeof(bool) || regularMember.Type == typeof(bool?)))
        {
            var memberName = GetMemberName(regularMember);
            return $"({memberName} = false)";
        }

        // 複雑な式の否定
        var operand = ProcessExpression(node.Operand);
        return $"NOT ({operand})";
    }

    /// <summary>
    /// メンバーアクセス処理
    /// </summary>
    private string HandleMemberAccess(MemberExpression node)
    {
        // Nullable<bool>の.Value アクセス
        if (node.Member.Name == "Value" &&
            node.Expression is MemberExpression innerMember &&
            innerMember.Type == typeof(bool?))
        {
            var memberName = GetMemberName(innerMember);
            return $"({memberName} = true)";
        }

        // HasValue プロパティアクセス
        if (node.Member.Name == "HasValue" &&
            node.Expression != null &&
            Nullable.GetUnderlyingType(node.Expression.Type) != null)
        {
            var memberName = GetMemberName((MemberExpression)node.Expression);
            return $"{memberName} IS NOT NULL";
        }

        // 通常のプロパティアクセス
        var finalMemberName = GetMemberName(node);

        // bool型プロパティは明示的に = true
        if (node.Type == typeof(bool) || node.Type == typeof(bool?))
        {
            return $"({finalMemberName} = true)";
        }

        return finalMemberName;
    }

    /// <summary>
    /// メソッド呼び出し処理
    /// </summary>
    private string HandleMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;

        // 文字列メソッドの特別処理
        switch (methodName)
        {
            case "Contains":
                return HandleContainsMethod(node);
            case "StartsWith":
                return HandleStartsWithMethod(node);
            case "EndsWith":
                return HandleEndsWithMethod(node);
            default:
                // 一般的な関数変換
                return KsqlFunctionTranslator.TranslateMethodCall(node);
        }
    }

    /// <summary>
    /// Contains メソッド処理
    /// </summary>
    private string HandleContainsMethod(MethodCallExpression node)
    {
        if (node.Object != null && node.Arguments.Count == 1)
        {
            var target = ProcessExpression(node.Object);
            var value = ProcessExpression(node.Arguments[0]);
            return $"INSTR({target}, {value}) > 0";
        }
        
        return KsqlFunctionTranslator.TranslateMethodCall(node);
    }

    /// <summary>
    /// StartsWith メソッド処理
    /// </summary>
    private string HandleStartsWithMethod(MethodCallExpression node)
    {
        if (node.Object != null && node.Arguments.Count == 1)
        {
            var target = ProcessExpression(node.Object);
            var value = ProcessExpression(node.Arguments[0]);
            return $"STARTS_WITH({target}, {value})";
        }
        
        return KsqlFunctionTranslator.TranslateMethodCall(node);
    }

    /// <summary>
    /// EndsWith メソッド処理
    /// </summary>
    private string HandleEndsWithMethod(MethodCallExpression node)
    {
        if (node.Object != null && node.Arguments.Count == 1)
        {
            var target = ProcessExpression(node.Object);
            var value = ProcessExpression(node.Arguments[0]);
            return $"ENDS_WITH({target}, {value})";
        }
        
        return KsqlFunctionTranslator.TranslateMethodCall(node);
    }

    /// <summary>
    /// 汎用式処理
    /// </summary>
    private string ProcessExpression(Expression expression)
    {
        return expression switch
        {
            MemberExpression member => GetMemberName(member),
            ConstantExpression constant => SafeToString(constant.Value),
            MethodCallExpression methodCall => KsqlFunctionTranslator.TranslateMethodCall(methodCall),
            BinaryExpression binary => ProcessBinaryExpression(binary),
            UnaryExpression unary when unary.NodeType == ExpressionType.Convert => ProcessExpression(unary.Operand),
            _ => expression.ToString()
        };
    }

    /// <summary>
    /// 二項式処理
    /// </summary>
    private string ProcessBinaryExpression(BinaryExpression binary)
    {
        var left = ProcessExpression(binary.Left);
        var right = ProcessExpression(binary.Right);
        var varoperator = GetSqlOperator(binary.NodeType);
        return $"({left} {varoperator} {right})";
    }

    /// <summary>
    /// メンバー名取得
    /// </summary>
    private static string GetMemberName(MemberExpression member)
    {
        // パラメーター接頭辞なしでプロパティ名のみ返す
        return member.Member.Name;
    }

    /// <summary>
    /// SQL演算子変換
    /// </summary>
    private static string GetSqlOperator(ExpressionType nodeType)
    {
        return nodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            ExpressionType.Add => "+",
            ExpressionType.Subtract => "-",
            ExpressionType.Multiply => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "%",
            _ => throw new NotSupportedException($"Operator {nodeType} is not supported in WHERE clause")
        };
    }

    /// <summary>
    /// NULL安全文字列変換
    /// </summary>
    private static string SafeToString(object? value)
    {
        return BuilderValidation.SafeToString(value);
    }
}
