using System;
using System.Collections.Generic;
using System.Linq;
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

    private class CategoryEntity
    {
        public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public int? NullableValue { get; set; }
        public List<int> List { get; set; } = new();
    }

    private static string Upper(string value) => value.ToUpperInvariant();

    [Fact]
    public void Visit_CompositeKeyWithCategory_ReturnsGroupByClause()
    {
        Expression<Func<CategoryEntity, object>> expr = e => new { e.Id, e.Category };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Id, Category", result);
    }

    [Fact]
    public void Visit_WithFunctionCall_TranslatesFunction()
    {
        Expression<Func<CategoryEntity, object>> expr = e => new { e.Id, Category = Upper(e.Category) };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Id, UPPER(Category)", result);
    }

    [Fact]
    public void Visit_NestedAnonymousType_FlattensMembers()
    {
        Expression<Func<CategoryEntity, object>> expr = e => new { e.Type, Sub = new { e.Id } };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("Type, Id", result);
    }

    [Fact]
    public void Visit_CoalesceExpression_TranslatesToCoalesce()
    {
        Expression<Func<CategoryEntity, object>> expr = e => e.NullableValue ?? 0;
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("COALESCE(NullableValue, 0)", result);
    }

    [Fact]
    public void Visit_ConstantExpression_ReturnsConstant()
    {
        Expression<Func<CategoryEntity, object>> expr = e => new { Value = 1 };
        var visitor = new GroupByExpressionVisitor();
        visitor.Visit(expr.Body);
        var result = visitor.GetResult();
        Assert.Equal("1", result);
    }

    [Fact]
    public void Visit_UnsupportedExpression_Throws()
    {
        Expression<Func<CategoryEntity, object>> expr = e => e.List.First();
        var visitor = new GroupByExpressionVisitor();
        Assert.Throws<NotSupportedException>(() => visitor.Visit(expr.Body));
    }
}
