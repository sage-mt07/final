using Kafka.Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class ExpressionAnalysisResultTests
{
    [Fact]
    public void HasJoin_PropertyRoundTrip()
    {
        var result = new ExpressionAnalysisResult { HasJoin = true };
        Assert.True(result.HasJoin);
    }
}
