using Kafka.Ksql.Linq.Query.Builders.Common;
using Kafka.Ksql.Linq.Query.Builders.Functions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;
/// <summary>
/// SELECT句専用ExpressionVisitor
/// </summary>
internal class SelectExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _columns = new();
    private readonly HashSet<string> _usedAliases = new();

    public string GetResult()
    {
        return _columns.Count > 0 ? string.Join(", ", _columns) : string.Empty;
    }

    protected override Expression VisitNew(NewExpression node)
    {
        // 匿名型の射影処理
        for (int i = 0; i < node.Arguments.Count; i++)
        {
            var arg = node.Arguments[i];
            var memberName = node.Members?[i]?.Name ?? $"col{i}";

            var columnExpression = ProcessProjectionArgument(arg);
            var alias = GenerateUniqueAlias(memberName);

            if (columnExpression != alias)
            {
                _columns.Add($"{columnExpression} AS {alias}");
            }
            else
            {
                _columns.Add(columnExpression);
            }
        }

        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // 単純なプロパティアクセス
        var columnName = GetColumnName(node);
        _columns.Add(columnName);
        return node;
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        // SELECT * の場合
        _columns.Add("*");
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // 関数呼び出しの処理
        var functionCall = KsqlFunctionTranslator.TranslateMethodCall(node);
        _columns.Add(functionCall);
        return node;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        // 計算式の処理
        var left = ProcessExpression(node.Left);
        var right = ProcessExpression(node.Right);
        var varoperator = GetOperator(node.NodeType);

        var expression = $"({left} {varoperator} {right})";
        _columns.Add(expression);
        return node;
    }

    protected override Expression VisitConditional(ConditionalExpression node)
    {
        // CASE式の処理
        var test = ProcessExpression(node.Test);
        var ifTrue = ProcessExpression(node.IfTrue);
        var ifFalse = ProcessExpression(node.IfFalse);

        var caseExpression = $"CASE WHEN {test} THEN {ifTrue} ELSE {ifFalse} END";
        _columns.Add(caseExpression);
        return node;
    }

    /// <summary>
    /// 射影引数処理
    /// </summary>
    private string ProcessProjectionArgument(Expression arg)
    {
        return arg switch
        {
            MemberExpression member => GetColumnName(member),
            MethodCallExpression methodCall => KsqlFunctionTranslator.TranslateMethodCall(methodCall),
            BinaryExpression binary => ProcessBinaryExpression(binary),
            ConstantExpression constant => SafeToString(constant.Value),
            ConditionalExpression conditional => ProcessConditionalExpression(conditional),
            UnaryExpression unary => ProcessExpression(unary.Operand),
            _ => ProcessExpression(arg)
        };
    }

    /// <summary>
    /// 二項式処理
    /// </summary>
    private string ProcessBinaryExpression(BinaryExpression binary)
    {
        var left = ProcessExpression(binary.Left);
        var right = ProcessExpression(binary.Right);
        var varoperator = GetOperator(binary.NodeType);
        return $"({left} {varoperator} {right})";
    }

    /// <summary>
    /// 条件式処理
    /// </summary>
    private string ProcessConditionalExpression(ConditionalExpression conditional)
    {
        var test = ProcessExpression(conditional.Test);
        var ifTrue = ProcessExpression(conditional.IfTrue);
        var ifFalse = ProcessExpression(conditional.IfFalse);
        return $"CASE WHEN {test} THEN {ifTrue} ELSE {ifFalse} END";
    }

    /// <summary>
    /// 汎用式処理
    /// </summary>
    private string ProcessExpression(Expression expression)
    {
        return expression switch
        {
            MemberExpression member => GetColumnName(member),
            ConstantExpression constant => SafeToString(constant.Value),
            MethodCallExpression methodCall => KsqlFunctionTranslator.TranslateMethodCall(methodCall),
            BinaryExpression binary => ProcessBinaryExpression(binary),
            UnaryExpression unary => ProcessExpression(unary.Operand),
            _ => expression.ToString()
        };
    }

    /// <summary>
    /// カラム名取得
    /// </summary>
    private static string GetColumnName(MemberExpression member)
    {
        // ネストしたプロパティアクセスの処理
        var path = new List<string>();
        var current = member;

        while (current != null)
        {
            path.Insert(0, current.Member.Name);
            current = current.Expression as MemberExpression;
        }

        // ルートがParameterの場合は最後の要素のみ使用
        if (member.Expression is ParameterExpression)
        {
            return member.Member.Name;
        }

        return string.Join(".", path);
    }

    /// <summary>
    /// 一意エイリアス生成
    /// </summary>
    private string GenerateUniqueAlias(string baseName)
    {
        var alias = baseName;
        var counter = 1;

        while (_usedAliases.Contains(alias))
        {
            alias = $"{baseName}_{counter}";
            counter++;
        }

        _usedAliases.Add(alias);
        return alias;
    }

    /// <summary>
    /// 演算子変換
    /// </summary>
    private static string GetOperator(ExpressionType nodeType)
    {
        return nodeType switch
        {
            ExpressionType.Add => "+",
            ExpressionType.Subtract => "-",
            ExpressionType.Multiply => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "%",
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            _ => throw new NotSupportedException($"Operator {nodeType} is not supported in SELECT clause")
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
