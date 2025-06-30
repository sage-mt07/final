using Kafka.Ksql.Linq.Query.Builders.Functions;
using Xunit;

namespace Kafka.Ksql.Linq.Query.Tests;

public class HavingClauseBuilderTests
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
        var result = KsqlFunctionRegistry.IsAggregateFunction(name);
        Assert.True(result);
    }

    [Fact]
    public void IsAggregateFunction_ReturnsFalseForUnknown()
    {
        var result = KsqlFunctionRegistry.IsAggregateFunction("LASTVALUE");
        Assert.False(result);
    }
}
