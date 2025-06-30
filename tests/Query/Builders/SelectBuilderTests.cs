using System;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class WhereClauseBuilderTests
{
    [Fact]
    public void Build_SimpleEquality_ReturnsWhereClause()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.Id == 1;
        var builder = new WhereClauseBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("(Id = 1)", result);
    }

    [Fact]
    public void BuildCondition_BooleanNegation_IncludesParameterPrefix()
    {
        Expression<Func<TestEntity, bool>> expr = e => !e.IsActive;
        var builder = new WhereClauseBuilder();
        var result = builder.BuildCondition(expr.Body);
        Assert.Equal("(e.IsActive = false)", result);
    }

    [Fact]
    public void Build_NullExpression_ThrowsArgumentNullException()
    {
        var builder = new WhereClauseBuilder();
        Assert.Throws<ArgumentNullException>(() => builder.Build(null!));
    }
}
