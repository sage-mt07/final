using System;
using Kafka.Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class QueryExecutionResultTests
{
    [Fact]
    public void PropertyRoundTrip()
    {
        var now = DateTime.UtcNow;
        var result = new QueryExecutionResult
        {
            Success = true,
            TargetObject = "obj",
            Data = new object(),
            ExecutedAt = now,
            ErrorMessage = "err"
        };
        Assert.True(result.Success);
        Assert.Equal("obj", result.TargetObject);
        Assert.Equal(now, result.ExecutedAt);
        Assert.NotNull(result.Data);
        Assert.Equal("err", result.ErrorMessage);
    }
}
