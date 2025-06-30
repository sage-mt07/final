using System;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Tests;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Visitors;

public class GroupByExpressionVisitorTests
{
    [Fact]
    public void Visit_CompositeKey_ReturnsCommaSeparatedKeys()
    {
        Expression<Func<TestEntity, object>> expr = e => new { e.Id, e.Type };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Id, Type", result);
    }

    private class Parent
    {
        public Child Child { get; set; } = new();
    }

    private class Child
    {
        public int Value { get; set; }
    }

    [Fact]
    public void Visit_NestedProperty_ReturnsLeafPropertyName()
    {
        Expression<Func<Parent, object>> expr = p => p.Child.Value;
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Value", result);
    }
}
