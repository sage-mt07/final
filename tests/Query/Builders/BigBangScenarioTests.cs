using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public record OrderCustomerInfo(DateTime OrderDate, decimal TotalAmount, string? Region);

public class BigBangScenarioTests
{
    [Fact]
    public void Build_ComplexQuery_GeneratesExpectedClauses()
    {
        IQueryable<Order> orders = new List<Order>().AsQueryable();
        IQueryable<Customer> customers = new List<Customer>().AsQueryable();

        // join expression
        var joinExpr = orders.Join(customers,
            o => o.CustomerId,
            c => c.Id,
            (o, c) => new OrderCustomerInfo(o.OrderDate, o.TotalAmount, c.Region)).Expression;
        var joinBuilder = new JoinBuilder();
        var joinSql = joinBuilder.Build(joinExpr);

        // where clause
        Expression<Func<OrderCustomerInfo, bool>> whereExp = x => x.TotalAmount > 100 && x.Region != null;
        var whereBuilder = new SelectBuilder();
        var whereSql = whereBuilder.Build(whereExp.Body);

        // group by
        Expression<Func<OrderCustomerInfo, object>> groupExp = x => x.OrderDate;
        var groupBuilder = new GroupByBuilder();
        var groupSql = groupBuilder.Build(groupExp.Body);

        // window clause
        Expression<Func<WindowDef, WindowDef>> winExp = w => w.TumblingWindow().Size(TimeSpan.FromDays(1));
        var windowBuilder = new WindowBuilder();
        var windowSql = windowBuilder.Build(winExp.Body);

        var final = $"{joinSql} {whereSql} {windowSql} {groupSql}";

        Assert.Contains("JOIN Customer", final);
        Assert.Contains("WHERE ((TotalAmount > 100) AND Region IS NOT NULL)", final);
        Assert.Contains("WINDOW TUMBLING (SIZE 1 DAYS)", final);
        Assert.Contains("GROUP BY OrderDate", final);
    }
}
