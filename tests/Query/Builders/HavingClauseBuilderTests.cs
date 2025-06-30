using System;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Tests;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class HavingClauseBuilderTests
{
    [Fact]
    public void Build_CountCondition_ReturnsCountExpression()
    {
        Expression<Func<IGrouping<int, TestEntity>, bool>> expr = g => g.Count() > 1;
        var builder = new HavingClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("(COUNT(*) > 1)", sql);
    }

    [Fact]
    public void Build_SumCondition_ReturnsSumExpression()
    {
        Expression<Func<IGrouping<int, TestEntity>, bool>> expr = g => g.Sum(x => x.Id) > 5;
        var builder = new HavingClauseBuilder();
        var sql = builder.Build(expr.Body);
        Assert.Equal("(SUM(Id) > 5)", sql);
    }
}
