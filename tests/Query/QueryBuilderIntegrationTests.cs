using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query;

public class QueryBuilderIntegrationTests
{
    [Fact]
    public void Build_WherePlusJoin_GeneratesClauses()
    {
        IQueryable<Order> orders = new List<Order>().AsQueryable();
        IQueryable<Customer> customers = new List<Customer>().AsQueryable();

        Expression<Func<Order, bool>> whereExp = o => o.TotalAmount > 1000;

        var joinExpr = orders.Join(
            customers,
            o => o.CustomerId,
            c => c.Id,
            (o, c) => new { o.OrderId, c.Region }
        ).Expression;

        var joinBuilder = new JoinClauseBuilder();
        var whereBuilder = new WhereClauseBuilder();
        var joinSql = joinBuilder.Build(joinExpr);
        var whereSql = whereBuilder.Build(whereExp.Body);
        var final = $"{joinSql} {whereSql}";

        Assert.Contains("JOIN", final);
        Assert.Contains("(TotalAmount > 1000)", final);
    }

    [Fact]
    public void Build_WindowAndGroupBy_GeneratesClauses()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src.Window(TumblingWindow.OfMinutes(5))
                      .GroupBy(x => x.Id)
                      .Select(g => new { g.Key, Count = g.Count() });

        var generator = new DDLQueryGenerator();
        var sql = generator.GenerateCreateTableAs("t1", "Base", expr.Expression);

        Assert.Contains("WINDOW TUMBLING (SIZE 5 MINUTES)", sql);
        Assert.Contains("GROUP BY Id", sql);
    }

    [Fact]
    public void Build_JoinWithAnonymousKeys_GeneratesCondition()
    {
        IQueryable<Order> orders = new List<Order>().AsQueryable();
        IQueryable<Payment> payments = new List<Payment>().AsQueryable();

        var expr = orders.Join(
            payments,
            o => new { o.OrderId },
            p => new { p.OrderId },
            (o, p) => new { o.OrderId, p.Status }
        ).Expression;

        var builder = new JoinClauseBuilder();
        var sql = builder.Build(expr);

        Assert.Contains("JOIN", sql);
        Assert.Contains("o.OrderId = p.OrderId", sql);
    }


    [Fact]
    public void Build_SelectAnonymousType_ReturnsSelectClause()
    {
        Expression<Func<Order, object>> expr = o => new { o.OrderId, o.TotalAmount, o.OrderDate };
        var builder = new SelectClauseBuilder();
        var sql = builder.Build(expr.Body);

        Assert.Equal("OrderId, TotalAmount, OrderDate", sql);
    }

    [Fact]
    public void Build_WindowThenOrderBy_IgnoresSort()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src.Window(TimeSpan.FromMinutes(1))
                      .OrderBy(x => x.Id)
                      .Select(x => x.Name);

        var generator = new DDLQueryGenerator();
        var sql = generator.GenerateCreateTableAs("t1", "Base", expr.Expression);

        Assert.Contains("WINDOW TUMBLING (SIZE 1 MINUTES)", sql);
        Assert.DoesNotContain("ORDER BY", sql);
    }

    [Fact]
    public void Build_GroupByAndHaving_GeneratesClauses()
    {
        Expression<Func<Order, object>> groupExpr = o => o.CustomerId;
        Expression<Func<IGrouping<int, Order>, bool>> havingExpr = g => g.Count() > 5;

        var groupBuilder = new GroupByClauseBuilder();
        var havingBuilder = new HavingClauseBuilder();

        var groupSql = groupBuilder.Build(groupExpr.Body);
        var havingSql = havingBuilder.Build(havingExpr.Body);

        var final = $"{groupSql} {havingSql}";

        Assert.Equal("CustomerId (COUNT(*) > 5)", final);
    }

    [Fact]
    public void Build_SelectAnonymousTypeWithConversion_ReturnsSelectClause()
    {
        Expression<Func<Order, object>> expr = o => new { o.OrderId, Date = o.OrderDate.Date };
        var builder = new SelectClauseBuilder();
        var sql = builder.Build(expr.Body);

        Assert.Equal("OrderId, OrderDate AS Date", sql);
    }
}
