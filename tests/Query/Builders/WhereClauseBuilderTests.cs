using System;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Tests;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class WhereClauseBuilderTests
{
    [Fact]
    public void BuildCondition_Negation_ReturnsEqualsFalse()
    {
        Expression<Func<TestEntity, bool>> expr = e => !e.IsActive;
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("(IsActive = false)", sql);
    }

    [Fact]
    public void BuildCondition_NullComparison_ReturnsIsNull()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.IsProcessed == null;
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("IsProcessed IS NULL", sql);
    }

    [Fact]
    public void BuildCondition_NotNullComparison_ReturnsIsNotNull()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.IsProcessed != null;
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("IsProcessed IS NOT NULL", sql);
    }

    [Fact]
    public void BuildCondition_AndCondition_ReturnsJoinedClause()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.Id == 1 && e.Name == "foo";
        var builder = new WhereClauseBuilder();
        var sql = builder.BuildCondition(expr.Body);
        Assert.Equal("((Id = 1) AND (Name = 'foo'))", sql);
    }
}
