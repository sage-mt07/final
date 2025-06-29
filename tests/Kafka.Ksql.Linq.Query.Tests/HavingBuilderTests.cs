using System.Reflection;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Tests;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Query.Tests;

public class HavingBuilderTests
{
    [Theory]
    [InlineData("MAX")]
    [InlineData("MIN")]
    [InlineData("AVG")]
    [InlineData("AVERAGE")]
    [InlineData("EARLIESTBYOFFSET")]
    [InlineData("LATESTBYOFFSET")]
    [InlineData("COLLECTSET")]
    public void IsAggregateFunction_ReturnsTrueForSupported(string name)
    {
        var visitorType = typeof(HavingBuilder)
            .GetNestedType("HavingExpressionVisitor", BindingFlags.NonPublic)!;
        var result = InvokePrivate<bool>(visitorType, "IsAggregateFunction", new[] { typeof(string) }, null, name);
        Assert.True(result);
    }

    [Fact]
    public void IsAggregateFunction_ReturnsFalseForUnknown()
    {
        var visitorType = typeof(HavingBuilder)
            .GetNestedType("HavingExpressionVisitor", BindingFlags.NonPublic)!;
        var result = InvokePrivate<bool>(visitorType, "IsAggregateFunction", new[] { typeof(string) }, null, "LASTVALUE");
        Assert.False(result);
    }
}
