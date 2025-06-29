using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq;
using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// WINDOW句構築ビルダー - 本体実装版
/// 設計理由：旧KsqlWindowBuilderへの中継を排除し、直接実装に移行
/// </summary>
internal class WindowBuilder : IKsqlBuilder
{
    public KsqlBuilderType BuilderType => KsqlBuilderType.Window;

    public string Build(Expression expression)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        var visitor = new WindowExpressionVisitor();

        switch (expression)
        {
            case ConstantExpression { Value: WindowDef def }:
                visitor.VisitWindowDef(def);
                break;
            case ConstantExpression { Value: TimeSpan ts }:
                visitor.VisitWindowDef(TumblingWindow.Of(ts));
                break;
            default:
                visitor.Visit(expression);
                break;
        }

        return visitor.BuildWindowClause();
    }

    /// <summary>
    /// WINDOW句専用ExpressionVisitor
    /// </summary>
    private class WindowExpressionVisitor : ExpressionVisitor
    {
        private string _windowType = "";
        private string _size = "";
        private string _advanceBy = "";
        private string _gap = "";
        private string _retention = "";
        private string _gracePeriod = "";
        private string _emitBehavior = ""; // "FINAL" or empty (default CHANGES)

        public void VisitWindowDef(WindowDef def)
        {
            foreach (var (Name, Value) in def.Operations)
            {
                switch (Name)
                {
                    case nameof(WindowDef.TumblingWindow):
                        _windowType = "TUMBLING";
                        break;
                    case nameof(WindowDef.HoppingWindow):
                        _windowType = "HOPPING";
                        break;
                    case nameof(WindowDef.SessionWindow):
                        _windowType = "SESSION";
                        break;
                    case nameof(WindowDef.Size):
                        _size = FormatTimeSpan((TimeSpan)Value!);
                        break;
                    case nameof(WindowDef.AdvanceBy):
                        _advanceBy = FormatTimeSpan((TimeSpan)Value!);
                        break;
                    case nameof(WindowDef.Gap):
                        _gap = FormatTimeSpan((TimeSpan)Value!);
                        break;
                    case nameof(WindowDef.Retention):
                        _retention = FormatTimeSpan((TimeSpan)Value!);
                        break;
                    case nameof(WindowDef.GracePeriod):
                        _gracePeriod = FormatTimeSpan((TimeSpan)Value!);
                        break;
                    case nameof(WindowDef.EmitFinal):
                        _emitBehavior = "FINAL";
                        break;
                }
            }
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // First visit the object to ensure we process method chains in the correct order
            if (node.Object != null)
            {
                Visit(node.Object);
            }

            var methodName = node.Method.Name;

            switch (methodName)
            {
                case "TumblingWindow":
                    _windowType = "TUMBLING";
                    break;

                case "HoppingWindow":
                    _windowType = "HOPPING";
                    break;

                case "SessionWindow":
                    _windowType = "SESSION";
                    break;

                case "Size":
                    _size = ExtractTimeSpanValue(node);
                    break;

                case "AdvanceBy":
                    _advanceBy = ExtractTimeSpanValue(node);
                    break;

                case "Gap":
                    _gap = ExtractTimeSpanValue(node);
                    break;

                case "Retention":
                    _retention = ExtractTimeSpanValue(node);
                    break;

                case "GracePeriod":
                    _gracePeriod = ExtractTimeSpanValue(node);
                    break;

                case "EmitFinal":
                    _emitBehavior = "FINAL";
                    break;

                case "Window":
                    _windowType = "TUMBLING";
                    if (node.Arguments.Count > 1)
                    {
                        _size = ExtractTimeSpanValue(node.Arguments[1]);
                    }
                    break;
            }

            return node;
        }

        private string ExtractTimeSpanValue(MethodCallExpression node)
        {
            if (node.Arguments.Count > 0)
            {
                return ExtractTimeSpanValue(node.Arguments[0]);
            }

            return "UNKNOWN";
        }

        private string ExtractTimeSpanValue(Expression arg)
        {
            // Handle TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(30), etc.
            if (arg is MethodCallExpression timeSpanCall && timeSpanCall.Method.DeclaringType == typeof(TimeSpan))
            {
                var value = ExtractConstantValue(timeSpanCall.Arguments[0]);
                var unit = timeSpanCall.Method.Name switch
                {
                    "FromMinutes" => "MINUTES",
                    "FromSeconds" => "SECONDS",
                    "FromHours" => "HOURS",
                    "FromDays" => "DAYS",
                    _ => "UNKNOWN"
                };
                return $"{value} {unit}";
            }

            // Handle direct constants
            if (arg is ConstantExpression constant && constant.Value is TimeSpan timeSpan)
            {
                return FormatTimeSpan(timeSpan);
            }

            return "UNKNOWN";
        }

        private string ExtractConstantValue(Expression expression)
        {
            if (expression is ConstantExpression constant)
            {
                return constant.Value?.ToString() ?? "0";
            }
            return "UNKNOWN";
        }

        private string FormatTimeSpan(TimeSpan timeSpan)
        {
            if (timeSpan.TotalDays >= 1)
                return $"{(int)timeSpan.TotalDays} DAYS";
            if (timeSpan.TotalHours >= 1)
                return $"{(int)timeSpan.TotalHours} HOURS";
            if (timeSpan.TotalMinutes >= 1)
                return $"{(int)timeSpan.TotalMinutes} MINUTES";
            if (timeSpan.TotalSeconds >= 1)
                return $"{(int)timeSpan.TotalSeconds} SECONDS";

            return "0 SECONDS";
        }

        public string BuildWindowClause()
        {
            return _windowType switch
            {
                "TUMBLING" => BuildTumblingClause(),
                "HOPPING" => BuildHoppingClause(),
                "SESSION" => BuildSessionClause(),
                _ => "WINDOW UNKNOWN"
            };
        }

        private string BuildTumblingClause()
        {
            var clause = $"WINDOW TUMBLING (SIZE {_size}";

            if (!string.IsNullOrEmpty(_retention))
                clause += $", RETENTION {_retention}";

            if (!string.IsNullOrEmpty(_gracePeriod))
                clause += $", GRACE PERIOD {_gracePeriod}";

            clause += ")";

            if (!string.IsNullOrEmpty(_emitBehavior))
                clause += $" EMIT {_emitBehavior}";

            return clause;
        }

        private string BuildHoppingClause()
        {
            var clause = $"WINDOW HOPPING (SIZE {_size}";

            if (!string.IsNullOrEmpty(_advanceBy))
                clause += $", ADVANCE BY {_advanceBy}";

            if (!string.IsNullOrEmpty(_retention))
                clause += $", RETENTION {_retention}";

            if (!string.IsNullOrEmpty(_gracePeriod))
                clause += $", GRACE PERIOD {_gracePeriod}";

            clause += ")";

            if (!string.IsNullOrEmpty(_emitBehavior))
                clause += $" EMIT {_emitBehavior}";

            return clause;
        }

        private string BuildSessionClause()
        {
            var clause = $"WINDOW SESSION (GAP {_gap})";

            // Note: SESSION windows do not support RETENTION, GRACE PERIOD, or EMIT FINAL
            // They always emit changes immediately when sessions close

            return clause;
        }
    }
}