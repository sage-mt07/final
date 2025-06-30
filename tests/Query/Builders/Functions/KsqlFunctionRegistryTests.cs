using Kafka.Ksql.Linq.Query.Builders.Functions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Functions;

public class KsqlFunctionRegistryTests
{
    [Theory]
    [InlineData("Sum", "DOUBLE")]
    [InlineData("Count", "BIGINT")]
    [InlineData("Max", "ANY")]
    [InlineData("Min", "ANY")]
    [InlineData("TopK", "ARRAY")]
    [InlineData("Histogram", "MAP")]
    [InlineData("FooBar", "UNKNOWN")]
    [InlineData("sum", "DOUBLE")]
    [InlineData("SuM", "DOUBLE")]
    public void InferTypeFromMethodName_ReturnsExpected(string methodName, string expected)
    {
        var result = KsqlFunctionRegistry.InferTypeFromMethodName(methodName);
        Assert.Equal(expected, result);
    }
}
