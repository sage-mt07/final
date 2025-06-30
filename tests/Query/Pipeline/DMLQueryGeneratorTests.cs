using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq;
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

    [Fact]
    public void GenerateLinqQuery_FullClauseCombination()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src
            .Where(e => e.IsActive)
            .Window(TumblingWindow.OfMinutes(5))
            .GroupBy(e => e.Type)
            .Having(g => g.Count() > 1)
            .Select(g => new { g.Key, Count = g.Count() })
            .OrderBy(x => x.Key);

        var generator = new DMLQueryGenerator();
        var query = generator.GenerateLinqQuery("s1", expr.Expression, false);

        Assert.Contains("SELECT g.Key", query) || Assert.Contains("SELECT Key", query);
        Assert.Contains("COUNT(*) AS Count", query);
        Assert.Contains("FROM s1", query);
        Assert.Contains("WHERE (IsActive = true)", query);
        Assert.Contains("WINDOW TUMBLING", query);
        Assert.Contains("GROUP BY Type", query);
        Assert.Contains("HAVING (COUNT(*) > 1)", query);
        Assert.Contains("ORDER BY", query);
        Assert.EndsWith("EMIT CHANGES", query);
    }
}
