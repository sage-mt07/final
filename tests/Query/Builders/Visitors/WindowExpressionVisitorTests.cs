using System;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Query.Builders;
using System.Linq.Expressions;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Visitors;

public class WindowExpressionVisitorTests
{
    [Fact]
    public void ProcessWindow_Tumbling_ReturnsTumblingClause()
    {
        var visitor = new WindowExpressionVisitor();
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.TumblingWindow), null });
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.Size), TimeSpan.FromMinutes(5) });
        var result = InvokePrivate<string>(visitor, "BuildWindowClause", Type.EmptyTypes);
        Assert.Equal("TUMBLING (SIZE 5 MINUTES)", result);
    }

    [Fact]
    public void ProcessWindow_Hopping_ReturnsHoppingClause()
    {
        var visitor = new WindowExpressionVisitor();
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.HoppingWindow), null });
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.Size), TimeSpan.FromMinutes(5) });
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.AdvanceBy), TimeSpan.FromMinutes(1) });
        var result = InvokePrivate<string>(visitor, "BuildWindowClause", Type.EmptyTypes);
        Assert.Equal("HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTES)", result);
    }

    [Fact]
    public void ProcessWindow_Session_ReturnsSessionClause()
    {
        var visitor = new WindowExpressionVisitor();
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.SessionWindow), null });
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.Gap), TimeSpan.FromMinutes(10) });
        var result = InvokePrivate<string>(visitor, "BuildWindowClause", Type.EmptyTypes);
        Assert.Equal("SESSION (GAP 10 MINUTES)", result);
    }

    [Fact]
    public void ProcessWindow_InvalidParameters_ThrowsInvalidOperation()
    {
        var builder = new WindowClauseBuilder();
        var def = HoppingWindow.Of(TimeSpan.Zero).AdvanceBy(TimeSpan.FromMinutes(1));
        var expr = Expression.Constant(def);
        Assert.Throws<InvalidOperationException>(() => builder.Build(expr));
    }

    [Fact]
    public void ProcessWindow_MultipleCalls_LastCallWins()
    {
        var visitor = new WindowExpressionVisitor();
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.TumblingWindow), null });
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.HoppingWindow), null });
        InvokePrivate(
            visitor,
            "ProcessWindowOperation",
            new[] { typeof(string), typeof(object) },
            args: new object?[] { nameof(WindowDef.Size), TimeSpan.FromMinutes(3) });
        var result = InvokePrivate<string>(visitor, "BuildWindowClause", Type.EmptyTypes);
        Assert.Equal("HOPPING (SIZE 3 MINUTES)", result);
    }
}

