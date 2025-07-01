using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using Kafka.Ksql.Linq.Query.Builders.Functions;

namespace Kafka.Ksql.Linq.Query.Builders;
/// <summary>
/// 集約関数検出Visitor
/// </summary>
internal class AggregateDetectionVisitor : ExpressionVisitor
{
    public bool HasAggregates { get; private set; }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;
        if (KsqlFunctionRegistry.IsAggregateFunction(methodName))
        {
            HasAggregates = true;
        }
        
        return base.VisitMethodCall(node);
    }
}
