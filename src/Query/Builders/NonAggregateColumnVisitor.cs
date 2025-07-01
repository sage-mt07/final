using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using Kafka.Ksql.Linq.Query.Builders.Functions;

namespace Kafka.Ksql.Linq.Query.Builders;
/// <summary>
/// 非集約カラム検出Visitor
/// </summary>
internal class NonAggregateColumnVisitor : ExpressionVisitor
{
    public bool HasNonAggregateColumns { get; private set; }
    private bool _insideAggregateFunction;

    protected override Expression VisitMember(MemberExpression node)
    {
        if (!_insideAggregateFunction && node.Expression is ParameterExpression)
        {
            HasNonAggregateColumns = true;
        }
        
        return base.VisitMember(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;
        var wasInsideAggregate = _insideAggregateFunction;
        
        if (KsqlFunctionRegistry.IsAggregateFunction(methodName))
        {
            _insideAggregateFunction = true;
        }
        
        var result = base.VisitMethodCall(node);
        _insideAggregateFunction = wasInsideAggregate;
        
        return result;
    }
}
