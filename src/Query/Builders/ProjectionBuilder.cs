using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Linq.Expressions;
using System.Text;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// 射影（SELECT列）構築ビルダー - 本体実装版
/// 設計理由：旧KsqlProjectionBuilderへの中継を排除し、直接実装に移行
/// </summary>
internal class ProjectionBuilder : IKsqlBuilder
{
    public KsqlBuilderType BuilderType => KsqlBuilderType.Projection;

    public string Build(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        var visitor = new ProjectionExpressionVisitor();
        visitor.Visit(expression);
        var result = visitor.ToString();

        return !string.IsNullOrEmpty(result) ? "SELECT " + result : "SELECT *";
    }

    /// <summary>
    /// 射影専用ExpressionVisitor
    /// </summary>
    private class ProjectionExpressionVisitor : ExpressionVisitor
    {
        private readonly StringBuilder _sb = new();

        protected override Expression VisitNew(NewExpression node)
        {
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var arg = node.Arguments[i];
                var alias = node.Members?[i]?.Name;

                if (arg is MemberExpression member)
                {
                    var memberName = GetRootMemberName(member);

                    if (!string.IsNullOrEmpty(alias) && alias != memberName)
                    {
                        _sb.Append($"{memberName} AS {alias}, ");
                    }
                    else
                    {
                        _sb.Append($"{memberName}, ");
                    }
                }
                else if (arg is UnaryExpression unary && unary.Operand is MemberExpression unaryMember)
                {
                    // Handle UnaryExpression wrapping (like type conversions)
                    var memberName = GetRootMemberName(unaryMember);

                    if (!string.IsNullOrEmpty(alias) && alias != memberName)
                    {
                        _sb.Append($"{memberName} AS {alias}, ");
                    }
                    else
                    {
                        _sb.Append($"{memberName}, ");
                    }
                }
                else
                {
                    // Handle other expression types (constants, method calls, etc.)
                    var aliasToUse = alias ?? $"expr{i}";
                    Visit(arg);
                    if (!string.IsNullOrEmpty(aliasToUse))
                    {
                        _sb.Append($" AS {aliasToUse}");
                    }
                    _sb.Append(", ");
                }
            }
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            _sb.Append(GetRootMemberName(node));
            return node;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            _sb.Append("*");
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            // Skip Convert operations and process the inner operand directly
            return Visit(node.Operand);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Type == typeof(string))
                _sb.Append($"'{node.Value}'");
            else if (node.Type == typeof(bool))
                _sb.Append(node.Value?.ToString()?.ToLower() ?? "false"); // null安全性を追加
            else
                _sb.Append(node.Value ?? "NULL"); // null安全性を追加
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var methodName = node.Method.Name.ToUpper();

            // Handle common KSQL functions that might appear in projections
            switch (methodName)
            {
                case "WINDOWSTART":
                    _sb.Append("WINDOWSTART");
                    break;
                case "WINDOWEND":
                    _sb.Append("WINDOWEND");
                    break;
                case "TOSTRING":
                    _sb.Append("CAST(");
                    Visit(node.Object ?? node.Arguments[0]);
                    _sb.Append(" AS VARCHAR)");
                    break;
                case "TOLOWER":
                    _sb.Append("LCASE(");
                    Visit(node.Object ?? node.Arguments[0]);
                    _sb.Append(")");
                    break;
                case "TOUPPER":
                    _sb.Append("UCASE(");
                    Visit(node.Object ?? node.Arguments[0]);
                    _sb.Append(")");
                    break;
                case "SUBSTRING":
                    _sb.Append("SUBSTRING(");
                    Visit(node.Object ?? node.Arguments[0]);
                    _sb.Append(", ");
                    Visit(node.Arguments[node.Object != null ? 0 : 1]);
                    if (node.Arguments.Count > (node.Object != null ? 1 : 2))
                    {
                        _sb.Append(", ");
                        Visit(node.Arguments[node.Object != null ? 1 : 2]);
                    }
                    _sb.Append(")");
                    break;
                default:
                    if (IsAggregateFunction(methodName))
                    {
                        methodName = TransformMethodName(methodName);

                        if (methodName == "COUNT" &&
                            (node.Arguments.Count == 0 ||
                             (node.Arguments.Count == 1 && !(node.Arguments[0] is LambdaExpression))))
                        {
                            _sb.Append("COUNT(*)");
                            break;
                        }

                        if (node.Arguments.Count == 1 && node.Arguments[0] is LambdaExpression lambda)
                        {
                            var memberExpr = ExtractMember(lambda.Body);
                            if (memberExpr != null)
                            {
                                _sb.Append($"{methodName}({memberExpr.Member.Name})");
                                break;
                            }
                        }

                        if (node.Method.IsStatic && node.Arguments.Count >= 2)
                        {
                            var staticLambda = ExtractLambda(node.Arguments[1]);
                            if (staticLambda != null)
                            {
                                var memberExpr = ExtractMember(staticLambda.Body);
                                if (memberExpr != null)
                                {
                                    _sb.Append($"{methodName}({memberExpr.Member.Name})");
                                    break;
                                }
                            }
                        }

                        if (node.Object is MemberExpression objMember)
                        {
                            _sb.Append($"{methodName}({objMember.Member.Name})");
                            break;
                        }

                        _sb.Append($"{methodName}(UNKNOWN)");
                        break;
                    }

                    // For unknown methods, just use the method name as a function
                    _sb.Append($"{methodName}(");
                    if (node.Object != null)
                    {
                        Visit(node.Object);
                        if (node.Arguments.Count > 0)
                            _sb.Append(", ");
                    }
                    for (int i = 0; i < node.Arguments.Count; i++)
                    {
                        Visit(node.Arguments[i]);
                        if (i < node.Arguments.Count - 1)
                            _sb.Append(", ");
                    }
                    _sb.Append(")");
                    break;
            }
            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            _sb.Append("(");
            Visit(node.Left);
            _sb.Append(" " + GetSqlOperator(node.NodeType) + " ");
            Visit(node.Right);
            _sb.Append(")");
            return node;
        }

        private static bool IsAggregateFunction(string methodName)
        {
            return methodName switch
            {
                "SUM" or "COUNT" or "MAX" or "MIN" or "AVG" or "AVERAGE" or
                "LATESTBYOFFSET" or "EARLIESTBYOFFSET" or
                "COLLECTLIST" or "COLLECTSET" => true,
                _ => false
            };
        }

        private static string TransformMethodName(string methodName)
        {
            return methodName switch
            {
                "LATESTBYOFFSET" => "LATEST_BY_OFFSET",
                "EARLIESTBYOFFSET" => "EARLIEST_BY_OFFSET",
                "COLLECTLIST" => "COLLECT_LIST",
                "COLLECTSET" => "COLLECT_SET",
                "AVERAGE" => "AVG",
                _ => methodName
            };
        }

        private static string GetRootMemberName(MemberExpression member)
        {
            while (member.Expression is MemberExpression inner)
            {
                member = inner;
            }
            return member.Member.Name;
        }

        private static MemberExpression? ExtractMember(Expression body)
        {
            return body switch
            {
                MemberExpression member => member,
                UnaryExpression unary => ExtractMember(unary.Operand),
                _ => null
            };
        }

        private static LambdaExpression? ExtractLambda(Expression expr)
        {
            return expr switch
            {
                LambdaExpression lambda => lambda,
                UnaryExpression { Operand: LambdaExpression lambda } => lambda,
                _ => null
            };
        }

        private static string GetSqlOperator(ExpressionType nodeType) => nodeType switch
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
            _ => throw new NotSupportedException($"Unsupported operator: {nodeType}")
        };

        public override string ToString()
        {
            return _sb.ToString().TrimEnd(',', ' ');
        }
    }
}