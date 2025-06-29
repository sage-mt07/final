using Kafka.Ksql.Linq.Messaging.Consumers.Core;
using Kafka.Ksql.Linq.Core.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Messaging.Consumers;

public class ConsumerInstanceTests
{
    [Fact]
    public void Properties_CanBeSet()
    {
        var inst = new ConsumerInstance
        {
            ConsumerKey = new ConsumerKey(typeof(string), "t", "g"),
            PooledConsumer = new PooledConsumer(),
            RentedAt = System.DateTime.UtcNow,
            IsActive = true
        };
        Assert.Equal("String:t:g", inst.ConsumerKey.ToString());
        Assert.NotNull(inst.PooledConsumer);
        Assert.True(inst.IsActive);
        Assert.True(inst.RentedAt <= System.DateTime.UtcNow);
    }
}
