using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;

namespace Kafka.Ksql.Linq.Query.Builders;
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
