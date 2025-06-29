using Kafka.Ksql.Linq.Query.Pipeline;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class QueryDiagnosticsTests
{
    [Fact]
    public void SetMetadata_StoresValue()
    {
        var diag = new QueryDiagnostics(new NullLoggerFactory());
        diag.SetMetadata("key", 123);
        Assert.True(diag.MetaData.TryGetValue("key", out var val));
        Assert.Equal(123, val);
    }

    [Fact]
    public void LogStep_And_MarkComplete_NoException()
    {
        var diag = new QueryDiagnostics(new NullLoggerFactory());
        diag.LogStep("step1");
        diag.MarkComplete();
    }
}
