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

        Assert.True(
            query.Contains("SELECT g.Key") || query.Contains("SELECT Key"));
        Assert.Contains("COUNT(*) AS Count", query);
        Assert.Contains("FROM s1", query);
        Assert.Contains("WHERE (IsActive = true)", query);
        Assert.Contains("WINDOW TUMBLING", query);
        Assert.Contains("GROUP BY Type", query);
        Assert.Contains("HAVING (COUNT(*) > 1)", query);
        Assert.Contains("ORDER BY", query);
        Assert.EndsWith("EMIT CHANGES", query);
    }

    private class Order
    {
        public int CustomerId { get; set; }
        public string Region { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public bool IsHighPriority { get; set; }
    }

    private class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void GenerateLinqQuery_GroupBySelectHaving_ComplexCondition()
    {
        IQueryable<Order> src = new List<Order>().AsQueryable();

        var expr = src
            .GroupBy(o => o.CustomerId)
            .Having(g => g.Count() > 10 && g.Sum(x => x.Amount) < 5000)
            .Select(g => new { g.Key, OrderCount = g.Count(), TotalAmount = g.Sum(x => x.Amount) });

        var generator = new DMLQueryGenerator();
        var query = generator.GenerateLinqQuery("Orders", expr.Expression, false);

        Assert.True(query.Contains("SELECT g.Key") || query.Contains("SELECT Key"));
        Assert.Contains("COUNT(*) AS OrderCount", query);
        Assert.Contains("SUM(Amount) AS TotalAmount", query);
        Assert.Contains("FROM Orders", query);
        Assert.Contains("GROUP BY CustomerId", query);
        Assert.Contains("HAVING ((COUNT(*) > 10) AND (SUM(Amount) < 5000))", query);
        Assert.EndsWith("EMIT CHANGES", query);
    }

    [Fact]
    public void GenerateLINQQuery_JoinGroupByHavingCondition_ReturnsExpectedQuery()
    {
        IQueryable<Order> orders = new List<Order>().AsQueryable();
        IQueryable<Customer> customers = new List<Customer>().AsQueryable();

        var expr = orders
            .Join(customers, o => o.CustomerId, c => c.Id, (o, c) => new { o, c })
            .GroupBy(x => x.o.CustomerId)
            .Having(g => g.Count() > 2 && g.Sum(x => x.o.Amount) < 10000)
            .Select(g => new
            {
                g.Key,
                OrderCount = g.Count(),
                TotalAmount = g.Sum(x => x.o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var query = generator.GenerateLinqQuery("Orders", expr.Expression, false);

        Assert.Contains("JOIN", query);
        Assert.Contains("GROUP BY CustomerId", query);
        Assert.Contains("HAVING ((COUNT(*) > 2) AND (SUM(Amount) < 10000))", query);
        Assert.Contains("COUNT(*) AS OrderCount", query);
        Assert.Contains("SUM(Amount) AS TotalAmount", query);
        Assert.EndsWith("EMIT CHANGES", query);
    }

    [Fact]
    public void GenerateLinqQuery_JoinGroupByHavingCondition_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();
        var customers = new List<Customer>().AsQueryable();

        var query =
            (from o in orders
             join c in customers on o.CustomerId equals c.Id
             group o by o.CustomerId into g
             select g)
            .Having(g => g.Count() > 2 && g.Sum(x => x.Amount) < 10000)
            .Select(g => new
            {
                g.Key,
                OrderCount = g.Count(),
                TotalAmount = g.Sum(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("joined", query.Expression, false);

        Assert.Contains("JOIN", result);
        Assert.Contains("GROUP BY", result);
        Assert.Contains("HAVING", result);
        Assert.Contains("COUNT(*)", result);
        Assert.Contains("SUM(", result);
        Assert.Contains("HAVING ((COUNT(*) > 2) AND (SUM(Amount) < 10000))", result);
        Assert.EndsWith("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByHavingWithMultipleAggregates_ReturnsExpectedQuery()
    {
        var src = new List<Order>().AsQueryable();

        var query = src
            .GroupBy(o => o.CustomerId)
            .Having(g => g.Average(x => x.Amount) > 100 && g.Max(x => x.Amount) < 1000)
            .Select(g => new
            {
                g.Key,
                OrderCount = g.Count(),
                TotalAmount = g.Sum(x => x.Amount),
                AvgAmount = g.Average(x => x.Amount),
                MaxAmount = g.Max(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("multiagg", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("HAVING", result);
        Assert.Contains("AVG(", result);
        Assert.Contains("MAX(", result);
        Assert.Contains("COUNT(*)", result);
        Assert.Contains("SUM(", result);
        Assert.Contains("HAVING ((AVG(Amount) > 100) AND (MAX(Amount) < 1000))", result);
        Assert.EndsWith("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_JoinWindowGroupByHavingCombination_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();
        var customers = new List<Customer>().AsQueryable();

        var query = orders
            .Join(
                customers,
                o => o.CustomerId,
                c => c.Id,
                (o, c) => new { o, c }
            )
            .Window(TumblingWindow.OfMinutes(10))
            .GroupBy(x => x.c.Name)
            .Having(g => g.Count() > 5 && g.Sum(x => x.o.Amount) > 1000)
            .Select(g => new
            {
                g.Key,
                OrderCount = g.Count(),
                TotalAmount = g.Sum(x => x.o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("joined", query.Expression, false);

        Assert.Contains("JOIN", result);
        Assert.Contains("WINDOW TUMBLING", result);
        Assert.Contains("GROUP BY", result);
        Assert.Contains("HAVING", result);
        Assert.Contains("COUNT(", result);
        Assert.Contains("SUM(", result);
        Assert.EndsWith("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_MultiKeyGroupByWithHaving_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => new { o.CustomerId, o.Region })
            .Having(g => g.Sum(x => x.Amount) > 1000)
            .Select(g => new
            {
                g.Key.CustomerId,
                g.Key.Region,
                Total = g.Sum(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("HAVING", result);
        Assert.Contains("CustomerId", result);
        Assert.Contains("Region", result);
        Assert.Contains("SUM", result);
        Assert.EndsWith("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithConditionalSum_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount),
                HighPriorityTotal = g.Sum(o => o.IsHighPriority ? o.Amount : 0)
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("CASE WHEN", result);
        Assert.Contains("SUM", result);
        Assert.Contains("HighPriorityTotal", result);
        Assert.Contains("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithAvgMinMax_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                AverageAmount = g.Average(o => o.Amount),
                MinAmount = g.Min(o => o.Amount),
                MaxAmount = g.Max(o => o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("AVG", result);
        Assert.Contains("MIN", result);
        Assert.Contains("MAX", result);
        Assert.Contains("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByAnonymousKeyWithKeyProjection_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => new { o.CustomerId, o.Region })
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("CustomerId", result);
        Assert.Contains("Region", result);
        Assert.Contains("SUM", result);
        Assert.Contains("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_GroupBySelectOrderBy_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount)
            })
            .OrderBy(x => x.Total);

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("SUM", result);
        Assert.Contains("ORDER BY", result);
        Assert.Contains("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_GroupBySelectOrderByDescending_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount)
            })
            .OrderByDescending(x => x.Total); // descending sort

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("SUM", result);
        Assert.Contains("ORDER BY", result);
        Assert.Contains("DESC", result);
        Assert.Contains("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_OrderByThenByDescending_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => new { o.CustomerId, o.Region })
            .Select(g => new
            {
                g.Key.CustomerId,
                g.Key.Region,
                Total = g.Sum(o => o.Amount)
            })
            .OrderBy(x => x.CustomerId)            // ascending
            .ThenByDescending(x => x.Total);       // descending

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("ORDER BY", result);
        Assert.Contains("CustomerId", result);
        Assert.Contains("Total", result);
        Assert.Contains("DESC", result);
        Assert.Contains("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_MultiKeyGroupByMultipleAggregates_HavingComplexConditions_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => new { o.CustomerId, o.Region })
            .Having(g => (g.Sum(x => x.Amount) > 1000 && g.Count() > 10) || g.Average(x => x.Amount) > 150)
            .Select(g => new
            {
                g.Key.CustomerId,
                g.Key.Region,
                TotalAmount = g.Sum(x => x.Amount),
                OrderCount = g.Count(),
                AverageAmount = g.Average(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("HAVING", result);
        Assert.Contains("SUM", result);
        Assert.Contains("COUNT", result);
        Assert.Contains("AVG", result);
        Assert.Contains("AND", result);
        Assert.Contains("OR", result);
        Assert.EndsWith("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithCaseWhen_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount),
                Status = g.Sum(o => o.Amount) > 1000 ? "VIP" : "Regular"
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("SUM", result);
        Assert.Contains("CASE", result);
        Assert.Contains("WHEN", result);
        Assert.Contains("THEN", result);
        Assert.Contains("ELSE", result);
        Assert.Contains("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithComplexHavingConditions_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Where(g =>
                (g.Sum(o => o.Amount) > 1000 && g.Count() > 5) ||
                g.Average(o => o.Amount) > 500)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount),
                Count = g.Count(),
                Avg = g.Average(o => o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("GROUP BY", result);
        Assert.Contains("HAVING", result);
        Assert.Contains("AND", result);
        Assert.Contains("OR", result);
        Assert.Contains("(", result);
        Assert.Contains(")", result);
        Assert.Contains("EMIT CHANGES", result);
    }

    [Fact]
    public void GenerateLinqQuery_WhereNotInClause_ReturnsExpectedQuery()
    {
        var excludedRegions = new[] { "CN", "RU" };
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .Where(o => !excludedRegions.Contains(o.Region))
            .Select(o => new
            {
                o.CustomerId,
                o.Region,
                o.Amount
            });

        var generator = new DMLQueryGenerator();
        var result = generator.GenerateLinqQuery("orders", query.Expression, false);

        Assert.Contains("WHERE", result);
        Assert.Contains("NOT IN", result);
        Assert.Contains("'CN'", result);
        Assert.Contains("'RU'", result);
        Assert.Contains("EMIT CHANGES", result);
    }
}
