using System;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class WindowClauseBuilderVisitWindowDefStateTests
{
    private static Type VisitorType => typeof(WindowClauseBuilder).GetNestedType("WindowExpressionVisitor", BindingFlags.NonPublic)!;

    private static object CreateVisitor() => Activator.CreateInstance(VisitorType)!;

    [Fact]
    public void VisitWindowDef_WhenTumblingWindowDefinitionProvided_ShouldSetFieldsForTumblingWindow()
    {
        var visitor = CreateVisitor();
        var def = new global::Kafka.Ksql.Linq.WindowDef()
            .TumblingWindow()
            .Size(TimeSpan.FromSeconds(30))
            .Retention(TimeSpan.FromDays(1))
            .GracePeriod(TimeSpan.FromMinutes(1))
            .EmitFinal();

        VisitorType.GetMethod("VisitWindowDef")!.Invoke(visitor, new object[] { def });

        Assert.Equal("TUMBLING", GetField<string>(visitor, "_windowType"));
        Assert.Equal("30 SECONDS", GetField<string>(visitor, "_size"));
        Assert.Equal("1 DAYS", GetField<string>(visitor, "_retention"));
        Assert.Equal("1 MINUTES", GetField<string>(visitor, "_gracePeriod"));
        Assert.Equal("FINAL", GetField<string>(visitor, "_emitBehavior"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_advanceBy"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_gap"));
    }

    [Fact]
    public void VisitWindowDef_WhenHoppingWindowDefinitionProvided_ShouldSetFieldsForHoppingWindow()
    {
        var visitor = CreateVisitor();
        var def = new global::Kafka.Ksql.Linq.WindowDef()
            .HoppingWindow()
            .Size(TimeSpan.FromMinutes(2))
            .AdvanceBy(TimeSpan.FromMinutes(1))
            .Retention(TimeSpan.FromHours(1))
            .GracePeriod(TimeSpan.FromSeconds(5))
            .EmitFinal();

        VisitorType.GetMethod("VisitWindowDef")!.Invoke(visitor, new object[] { def });

        Assert.Equal("HOPPING", GetField<string>(visitor, "_windowType"));
        Assert.Equal("2 MINUTES", GetField<string>(visitor, "_size"));
        Assert.Equal("1 MINUTES", GetField<string>(visitor, "_advanceBy"));
        Assert.Equal("1 HOURS", GetField<string>(visitor, "_retention"));
        Assert.Equal("5 SECONDS", GetField<string>(visitor, "_gracePeriod"));
        Assert.Equal("FINAL", GetField<string>(visitor, "_emitBehavior"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_gap"));
    }

    [Fact]
    public void VisitWindowDef_WhenSessionWindowDefinitionProvided_ShouldSetFieldsForSessionWindow()
    {
        var visitor = CreateVisitor();
        var def = new global::Kafka.Ksql.Linq.WindowDef()
            .SessionWindow()
            .Gap(TimeSpan.FromSeconds(45));

        VisitorType.GetMethod("VisitWindowDef")!.Invoke(visitor, new object[] { def });

        Assert.Equal("SESSION", GetField<string>(visitor, "_windowType"));
        Assert.Equal("45 SECONDS", GetField<string>(visitor, "_gap"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_size"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_advanceBy"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_retention"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_gracePeriod"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_emitBehavior"));
    }

    [Fact]
    public void VisitWindowDef_WhenNoWindowTypeProvided_ShouldKeepUnknownWindowType()
    {
        var visitor = CreateVisitor();
        var def = new global::Kafka.Ksql.Linq.WindowDef()
            .Size(TimeSpan.FromSeconds(10));

        VisitorType.GetMethod("VisitWindowDef")!.Invoke(visitor, new object[] { def });

        Assert.Equal(string.Empty, GetField<string>(visitor, "_windowType"));
        Assert.Equal("10 SECONDS", GetField<string>(visitor, "_size"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_advanceBy"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_gap"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_retention"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_gracePeriod"));
        Assert.Equal(string.Empty, GetField<string>(visitor, "_emitBehavior"));
    }

    private static T GetField<T>(object obj, string name)
    {
        var field = VisitorType.GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (T)field.GetValue(obj)!;
    }
}
