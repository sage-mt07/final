using System;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class DMLQueryGeneratorTests
{
    [Fact]
    public void GenerateSelectAll_WithPushQuery_AppendsEmitChanges()
    {
        var generator = new DMLQueryGenerator();
        var query = generator.GenerateSelectAll("s1", isPullQuery: false);
        Assert.Equal("SELECT * FROM s1 EMIT CHANGES", query);
    }

    [Fact]
    public void GenerateSelectWithCondition_Basic()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.Id == 1;
        var generator = new DMLQueryGenerator();
        var query = generator.GenerateSelectWithCondition("s1", expr.Body, false);
        Assert.Equal("SELECT * FROM s1 WHERE (Id = 1) EMIT CHANGES", query);
    }

    [Fact]
    public void GenerateCountQuery_ReturnsExpected()
    {
        var generator = new DMLQueryGenerator();
        var query = generator.GenerateCountQuery("t1");
        Assert.Equal("SELECT COUNT(*) FROM t1", query);
    }

    [Fact]
    public void GenerateAggregateQuery_Basic()
    {
        Expression<Func<TestEntity, object>> expr = e => new { Sum = e.Id };
        var generator = new DMLQueryGenerator();
        var query = generator.GenerateAggregateQuery("t1", expr.Body);
        Assert.Contains("FROM t1", query);
        Assert.StartsWith("SELECT", query);
    }

    [Fact]
    public void GenerateAggregateQuery_LatestByOffset()
    {
        Expression<Func<IGrouping<int, TestEntity>, object>> expr = g => new { Last = g.LatestByOffset(x => x.Id) };
        var generator = new DMLQueryGenerator();
        var query = generator.GenerateAggregateQuery("t1", expr.Body);
        Assert.Equal("SELECT LATEST_BY_OFFSET(Id) AS Last FROM t1", query);
    }

    [Fact]
    public void GenerateAggregateQuery_EarliestByOffset()
    {
        Expression<Func<IGrouping<int, TestEntity>, object>> expr = g => new { First = g.EarliestByOffset(x => x.Id) };
        var generator = new DMLQueryGenerator();
        var query = generator.GenerateAggregateQuery("t1", expr.Body);
        Assert.Equal("SELECT EARLIEST_BY_OFFSET(Id) AS First FROM t1", query);
    }
}
