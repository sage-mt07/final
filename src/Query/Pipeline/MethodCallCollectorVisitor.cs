using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
internal class MethodCallCollectorVisitor : ExpressionVisitor
{
    public List<MethodCallExpression> MethodCalls { get; } = new();

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        MethodCalls.Add(node);
        return base.VisitMethodCall(node);
    }
}
