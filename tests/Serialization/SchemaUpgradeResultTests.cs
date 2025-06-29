using System;
using Kafka.Ksql.Linq.Serialization.Avro.Management;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Serialization;

public class SchemaUpgradeResultTests
{
    [Fact]
    public void Properties_Work()
    {
        var now = DateTime.UtcNow;
        var result = new SchemaUpgradeResult
        {
            Success = true,
            NewSchemaId = 3,
            Reason = "ok",
            UpgradedAt = now
        };
        Assert.True(result.Success);
        Assert.Equal(3, result.NewSchemaId);
        Assert.Equal("ok", result.Reason);
        Assert.Equal(now, result.UpgradedAt);
    }
}
