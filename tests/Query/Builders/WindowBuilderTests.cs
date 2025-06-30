using System;
using System.Linq.Expressions;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class WindowClauseBuilderTests
{
    [Fact]
    public void Build_TumblingWindowWithFinal_ReturnsClause()
    {
        Expression<Func<WindowDef, WindowDef>> expr = w => w.TumblingWindow().Size(TimeSpan.FromMinutes(1)).EmitFinal();
        var builder = new WindowClauseBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("TUMBLING (SIZE 1 MINUTES) EMIT FINAL", result);
    }

    [Fact]
    public void Build_NullExpression_ThrowsArgumentNullException()
    {
        var builder = new WindowClauseBuilder();
        Assert.Throws<ArgumentNullException>(() => builder.Build(null!));
    }

    [Fact]
    public void VisitMethodCall_BuildsTumblingWindowClause()
    {
        Expression<Func<WindowDef, WindowDef>> expr = w => w.TumblingWindow().Size(TimeSpan.FromSeconds(30)).EmitFinal();
        var visitorType = typeof(WindowClauseBuilder).GetNestedType("WindowExpressionVisitor", BindingFlags.NonPublic)!;
        var visitor = Activator.CreateInstance(visitorType)!;
        InvokePrivate<object>(visitor, "Visit", new[] { typeof(Expression) }, null, expr.Body);
        var result = (string)visitorType.GetMethod("BuildWindowClause")!.Invoke(visitor, null)!;
        Assert.Equal("TUMBLING (SIZE 30 SECONDS) EMIT FINAL", result);
    }

    [Fact]
    public void VisitWindowDef_TumblingWindow_AllOptions()
    {
        var visitorType = typeof(WindowClauseBuilder).GetNestedType("WindowExpressionVisitor", BindingFlags.NonPublic)!;
        var visitor = Activator.CreateInstance(visitorType)!;

        var def = new global::Kafka.Ksql.Linq.WindowDef()
            .TumblingWindow()
            .Size(TimeSpan.FromSeconds(30))
            .Retention(TimeSpan.FromDays(1))
            .GracePeriod(TimeSpan.FromMinutes(1))
            .EmitFinal();

        visitorType.GetMethod("VisitWindowDef")!.Invoke(visitor, new object[] { def });
        var result = (string)visitorType.GetMethod("BuildWindowClause")!.Invoke(visitor, null)!;

        Assert.Equal("TUMBLING (SIZE 30 SECONDS, RETENTION 1 DAYS, GRACE PERIOD 1 MINUTES) EMIT FINAL", result);
    }

    [Fact]
    public void VisitWindowDef_HoppingWindow_AllOptions()
    {
        var visitorType = typeof(WindowClauseBuilder).GetNestedType("WindowExpressionVisitor", BindingFlags.NonPublic)!;
        var visitor = Activator.CreateInstance(visitorType)!;

        var def = new global::Kafka.Ksql.Linq.WindowDef()
            .HoppingWindow()
            .Size(TimeSpan.FromMinutes(2))
            .AdvanceBy(TimeSpan.FromMinutes(1))
            .Retention(TimeSpan.FromHours(1))
            .GracePeriod(TimeSpan.FromSeconds(5))
            .EmitFinal();

        visitorType.GetMethod("VisitWindowDef")!.Invoke(visitor, new object[] { def });
        var result = (string)visitorType.GetMethod("BuildWindowClause")!.Invoke(visitor, null)!;

        Assert.Equal("HOPPING (SIZE 2 MINUTES, ADVANCE BY 1 MINUTES, RETENTION 1 HOURS, GRACE PERIOD 5 SECONDS) EMIT FINAL", result);
    }

    [Fact]
    public void VisitWindowDef_SessionWindow_Basic()
    {
        var visitorType = typeof(WindowClauseBuilder).GetNestedType("WindowExpressionVisitor", BindingFlags.NonPublic)!;
        var visitor = Activator.CreateInstance(visitorType)!;

        var def = new global::Kafka.Ksql.Linq.WindowDef()
            .SessionWindow()
            .Gap(TimeSpan.FromSeconds(45));

        visitorType.GetMethod("VisitWindowDef")!.Invoke(visitor, new object[] { def });
        var result = (string)visitorType.GetMethod("BuildWindowClause")!.Invoke(visitor, null)!;

        Assert.Equal("SESSION (GAP 45 SECONDS)", result);
    }
}
