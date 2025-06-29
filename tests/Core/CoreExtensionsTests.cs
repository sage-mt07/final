using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Extensions;
using System.Reflection;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Core;

public class CoreExtensionsTests
{
    private class Sample
    {
        [Key(Order = 1)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void GetTopicName_ReturnsAttributeValue()
    {
        var model = new EntityModel
        {
            EntityType = typeof(Sample),
            TopicAttribute = new TopicAttribute("topic"),
            KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! },
            AllProperties = typeof(Sample).GetProperties()
        };
        Assert.Equal("topic", model.GetTopicName());
        Assert.True(model.HasKeys());
        Assert.False(model.IsCompositeKey());
        var ordered = model.GetOrderedKeyProperties();
        Assert.Single(ordered);
    }

    [Fact]
    public void TypeExtension_ReturnsTopicName()
    {
        Assert.Equal("Sample", typeof(Sample).GetKafkaTopicName());
        Assert.True(typeof(Sample).HasKafkaKeys());
    }

    [Fact]
    public void PropertyExtensions_DetectKey()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.Id))!;
        Assert.True(prop.IsKafkaKey());
        Assert.Equal(1, prop.GetKeyOrder());
    }
}
