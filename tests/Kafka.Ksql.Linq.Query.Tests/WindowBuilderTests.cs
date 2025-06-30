using System;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Tests;
using Xunit;

namespace Kafka.Ksql.Linq.Query.Tests;

public class WindowClauseBuilderTests
{
    [Fact]
    public void Build_SessionWindowWithGap_ReturnsClause()
    {
        Expression<Func<WindowDef, WindowDef>> expr = w => w.SessionWindow().Gap(TimeSpan.FromMinutes(2));
        var builder = new WindowClauseBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("WINDOW SESSION (GAP 2 MINUTES)", result);
    }

    [Fact]
    public void Build_HoppingWindowWithAdvance_ReturnsClause()
    {
        Expression<Func<WindowDef, WindowDef>> expr = w => w.HoppingWindow().Size(TimeSpan.FromMinutes(2)).AdvanceBy(TimeSpan.FromMinutes(1));
        var builder = new WindowClauseBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("WINDOW HOPPING (SIZE 2 MINUTES, ADVANCE BY 1 MINUTES)", result);
    }
}
