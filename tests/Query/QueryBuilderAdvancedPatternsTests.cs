using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Pipeline;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query;

public class QueryBuilderAdvancedPatternsTests
{
    [Fact]
    public void Build_WindowThenOrderBy_GeneratesWindowClause()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src.Window(TimeSpan.FromMinutes(1))
                      .OrderBy(e => e.Id)
                      .Select(e => e.Name);

        var generator = new DDLQueryGenerator(new NullLoggerFactory());
        var sql = generator.GenerateCreateTableAs("t1", "Base", expr.Expression);

        Assert.Contains("WINDOW TUMBLING (SIZE 1 MINUTES)", sql);
        Assert.DoesNotContain("ORDER BY", sql);
    }

    [Fact]
    public void Build_GroupByWithHaving_GeneratesClauses()
    {
        Expression<Func<Order, object>> groupExpr = o => o.CustomerId;
        Expression<Func<IGrouping<int, Order>, bool>> havingExpr = g => g.Count() > 10;

        var groupBuilder = new GroupByClauseBuilder();
        var havingBuilder = new HavingClauseBuilder();

        var groupSql = groupBuilder.Build(groupExpr.Body);
        var havingSql = havingBuilder.Build(havingExpr.Body);
        var final = $"{groupSql} {havingSql}";

        Assert.Equal("GROUP BY CustomerId HAVING (COUNT(*) > 10)", final);
    }

    [Fact]
    public void Build_AnonymousSelectionWithConversion_GeneratesSelectClause()
    {
        Expression<Func<Order, object>> expr = o => new { o.OrderId, Date = o.OrderDate.Date };
        var builder = new SelectClauseBuilder();
        var sql = builder.Build(expr.Body);

        Assert.Equal("SELECT OrderId, OrderDate AS Date", sql);
    }
}
