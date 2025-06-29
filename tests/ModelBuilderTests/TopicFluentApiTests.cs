using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.ModelBuilderTests;

public class TopicFluentApiTests
{
    private class Order
    {
        [Key]
        public int Id { get; set; }
    }

    [Fact]
    public void FluentApi_ConfiguresTopicSettings()
    {
        var builder = new ModelBuilder();
        builder.Entity<Order>()
            .AsTable("orders")
            .WithPartitions(3)
            .WithReplicationFactor(2)
            .WithPartitioner("custom");

        var model = builder.GetEntityModel<Order>();
        Assert.NotNull(model.TopicAttribute);
        Assert.Equal("orders", model.TopicAttribute!.TopicName);
        Assert.Equal(3, model.TopicAttribute.PartitionCount);
        Assert.Equal(2, model.TopicAttribute.ReplicationFactor);
        Assert.Equal("custom", model.TopicAttribute.Partitioner);
    }
}
