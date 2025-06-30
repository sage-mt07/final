using System;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class WindowClauseBuilderVisitWindowDefTests
{
    [Fact]
    public void VisitWindowDef_WhenNoWindowTypeDefined_ShouldReturnUnknownClause()
    {
        var visitorType = typeof(WindowClauseBuilder).GetNestedType("WindowExpressionVisitor", BindingFlags.NonPublic)!;
        var visitor = Activator.CreateInstance(visitorType)!;

        var def = new global::Kafka.Ksql.Linq.WindowDef()
            .Size(TimeSpan.FromSeconds(10));

        visitorType.GetMethod("VisitWindowDef")!.Invoke(visitor, new object[] { def });
        var result = (string)visitorType.GetMethod("BuildWindowClause")!.Invoke(visitor, null)!;

        Assert.Equal("WINDOW UNKNOWN", result);
    }

    [Fact]
    public void VisitWindowDef_WhenTumblingWindowWithFullOptions_ShouldBuildExpectedClause()
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

        Assert.Equal("WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1 DAYS, GRACE PERIOD 1 MINUTES) EMIT FINAL", result);
    }

    [Fact]
    public void VisitWindowDef_WhenHoppingWindowWithFullOptions_ShouldBuildExpectedClause()
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

        Assert.Equal("WINDOW HOPPING (SIZE 2 MINUTES, ADVANCE BY 1 MINUTES, RETENTION 1 HOURS, GRACE PERIOD 5 SECONDS) EMIT FINAL", result);
    }

    [Fact]
    public void VisitWindowDef_WhenSessionWindowWithGapSpecified_ShouldBuildExpectedClause()
    {
        var visitorType = typeof(WindowClauseBuilder).GetNestedType("WindowExpressionVisitor", BindingFlags.NonPublic)!;
        var visitor = Activator.CreateInstance(visitorType)!;

        var def = new global::Kafka.Ksql.Linq.WindowDef()
            .SessionWindow()
            .Gap(TimeSpan.FromSeconds(45));

        visitorType.GetMethod("VisitWindowDef")!.Invoke(visitor, new object[] { def });
        var result = (string)visitorType.GetMethod("BuildWindowClause")!.Invoke(visitor, null)!;

        Assert.Equal("WINDOW SESSION (GAP 45 SECONDS)", result);
    }
}
